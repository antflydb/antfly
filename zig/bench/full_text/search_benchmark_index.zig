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
const builtin = @import("builtin");
const common = @import("search_benchmark_common.zig");

const batch_size = 5000;
const merge_settlement_interval = 100_000;

pub fn main(init: std.process.Init) !void {
    // Timed production benchmark: allocator diagnostics belong in tests and
    // opt-in profiles, not in index-build latency or peak-RSS measurements.
    const alloc = std.heap.c_allocator;

    const args = try common.parseArgs(init.minimal.args);
    common.configureBenchmarkMergePolicy(args);
    common.configureBenchmarkIndexFormat(args);
    const index_start_ns = common.antfly.platform_time.monotonicNs();
    var db = common.openDb(alloc, args.db_path) catch |err| {
        std.debug.print("open benchmark DB failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer db.close();
    common.ensureIndex(&db) catch |err| {
        std.debug.print("create benchmark index failed: {s}\n", .{@errorName(err)});
        return err;
    };

    var stdin_buf: [64 * 1024]u8 = undefined;
    var reader = std.Io.File.stdin().readerStreaming(init.io, &stdin_buf);
    var line_buffer = try std.Io.Writer.Allocating.initCapacity(alloc, stdin_buf.len);
    defer line_buffer.deinit();
    var stderr_buf: [4096]u8 = undefined;
    var stderr = std.Io.File.stderr().writerStreaming(init.io, &stderr_buf);
    defer stderr.interface.flush() catch {};

    var batch_arena = std.heap.ArenaAllocator.init(alloc);
    defer batch_arena.deinit();
    var docs = std.ArrayListUnmanaged(common.antfly.introducer.TextDocument).empty;
    defer docs.deinit(alloc);

    var indexed: usize = 0;
    var generated_id: usize = 0;
    var normalized_bytes: u64 = 0;
    var empty_text_documents: usize = 0;
    var truncated_documents: usize = 0;
    var corpus_hasher = std.crypto.hash.sha2.Sha256.init(.{});
    while (true) {
        line_buffer.clearRetainingCapacity();
        var at_eof = false;
        _ = reader.interface.streamDelimiter(&line_buffer.writer, '\n') catch |err| switch (err) {
            error.EndOfStream => at_eof = true,
            else => return err,
        };
        if (!at_eof) reader.interface.toss(1);
        const line_raw = line_buffer.written();
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0) {
            if (at_eof) break;
            continue;
        }
        corpus_hasher.update(line);
        corpus_hasher.update("\n");
        normalized_bytes = std.math.add(u64, normalized_bytes, @as(u64, @intCast(line.len + 1))) catch return error.CorpusTooLarge;
        if (generated_id >= std.math.maxInt(u32)) return error.CorpusOrdinalOverflow;

        const batch_alloc = batch_arena.allocator();
        const doc = try normalizedDocument(batch_alloc, line, generated_id, args.max_text_bytes);
        if (doc.empty_text) empty_text_documents += 1;
        if (doc.truncated) truncated_documents += 1;
        generated_id += 1;
        const fields = try batch_alloc.alloc(common.antfly.introducer.TextField, 1);
        fields[0] = .{ .field_name = common.text_field, .text = doc.text };
        try docs.append(alloc, .{
            .id = doc.key,
            // A valid minimal source keeps the production segment writer's
            // stored-field contract without retaining the benchmark body.
            .stored_data = "{}",
            .text_fields = fields,
            .doc_ordinal = @as(u32, @intCast(generated_id)),
        });

        if (docs.items.len >= batch_size) {
            flushBatch(&db, docs.items) catch |err| {
                try stderr.interface.print("index batch failed after {d} documents: {s}\n", .{ indexed, @errorName(err) });
                try stderr.interface.flush();
                return err;
            };
            indexed += docs.items.len;
            docs.clearRetainingCapacity();
            _ = batch_arena.reset(.retain_capacity);
            if (indexed % merge_settlement_interval == 0) {
                // Keep the deterministic synchronous benchmark from building
                // an unbounded end-of-load merge backlog. This models the
                // production merge lane making progress during a long load.
                try db.drainScheduledTextMerges();
                var progress_layout = try db.textIndexLayoutStats(alloc, common.index_name);
                defer progress_layout.deinit(alloc);
                try stderr.interface.print(
                    "indexed={d} segments={d} elapsed_s={d:.1}\n",
                    .{ indexed, progress_layout.segments.len, @as(f64, @floatFromInt(common.antfly.platform_time.monotonicNs() - index_start_ns)) / std.time.ns_per_s },
                );
                try stderr.interface.flush();
            }
        }
        if (at_eof) break;
    }

    if (docs.items.len > 0) {
        flushBatch(&db, docs.items) catch |err| {
            try stderr.interface.print("final index batch failed after {d} documents: {s}\n", .{ indexed, @errorName(err) });
            try stderr.interface.flush();
            return err;
        };
        indexed += docs.items.len;
    }

    try stderr.interface.print("settling index after {d} documents\n", .{indexed});
    try stderr.interface.flush();
    db.drainScheduledTextMerges() catch |err| {
        try stderr.interface.print("final merge settlement failed: {s}\n", .{@errorName(err)});
        try stderr.interface.flush();
        return err;
    };
    if (args.segment_mode == .single) try db.forceCompactTextIndexes();
    var layout = try db.textIndexLayoutStats(alloc, common.index_name);
    defer layout.deinit(alloc);
    if (args.segment_mode == .single and layout.segments.len != 1) return error.SingleSegmentInvariantFailed;

    var corpus_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    corpus_hasher.final(&corpus_digest);
    const corpus_sha256 = std.fmt.bytesToHex(corpus_digest, .lower);
    const production_index_config = common.antfly.inverted.productionIndexConfig();
    const manifest = .{
        .schema_version = common.benchmark_schema_version,
        .result_schema_version = common.result_schema_version,
        .query_grammar = common.query_grammar_version,
        .corpus = .{
            .normalization = "trim_ascii_whitespace_drop_blank_append_lf_json_or_plain_text",
            .sha256 = corpus_sha256,
            .uncompressed_bytes = normalized_bytes,
            .input_documents = generated_id,
            .indexed_documents = indexed,
            .rejected_documents = @as(u32, 0),
            .empty_text_documents = empty_text_documents,
            .truncated_documents = truncated_documents,
            .max_text_bytes = args.max_text_bytes,
            .ordinal_assignment = "zero_based_normalized_input_order_u32",
        },
        .segment_mode = @tagName(args.segment_mode),
        .indexing_scope = "production_full_text_index_only",
        .stored_source = "omitted_index_only_ordinal_sidecar",
        .ingestion_batch_size = batch_size,
        .merge_settlement_interval_documents = merge_settlement_interval,
        .inverted_wire_version = production_index_config.wireVersion(),
        .postings_layout = production_index_config.postingsLayoutName(),
        .postings_chunk_size = production_index_config.chunk_size,
        .analysis = .{
            .name = common.analyzer_name,
            .tokenizer = "unicode_words",
            .filters = &.{"ascii_lowercase"},
            .stop_words = false,
            .stemming = false,
        },
        .bm25 = .{
            .k1 = args.bm25_k1,
            .b = args.bm25_b,
        },
        .index_elapsed_ns = common.antfly.platform_time.monotonicNs() - index_start_ns,
        .process_cpu_ns = processCpuNs(),
        .process_peak_rss_bytes = processPeakRssBytes(),
        .layout = layout,
    };
    const manifest_json = try std.json.Stringify.valueAlloc(alloc, manifest, .{ .whitespace = .indent_2 });
    defer alloc.free(manifest_json);
    if (args.manifest_path) |manifest_path| {
        try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = manifest_path, .data = manifest_json });
    }
    try stderr.interface.print("ANTFLY_SEARCH_BENCH_MANIFEST {s}\n", .{manifest_json});
    try stderr.interface.print("indexed {d} documents in {d} segments\n", .{ indexed, layout.segments.len });
}

fn timevalNs(value: anytype) u64 {
    if (value.sec < 0 or value.usec < 0) return 0;
    const seconds: u64 = @intCast(value.sec);
    const microseconds: u64 = @intCast(value.usec);
    const seconds_ns = std.math.mul(u64, seconds, std.time.ns_per_s) catch std.math.maxInt(u64);
    const microseconds_ns = std.math.mul(u64, microseconds, std.time.ns_per_us) catch std.math.maxInt(u64);
    return seconds_ns +| microseconds_ns;
}

fn processCpuNs() u64 {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    return timevalNs(usage.utime) +| timevalNs(usage.stime);
}

fn processPeakRssBytes() usize {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    if (usage.maxrss <= 0) return 0;
    const maxrss: usize = @intCast(usage.maxrss);
    return switch (builtin.os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(usize, maxrss, 1024) catch std.math.maxInt(usize),
        else => maxrss,
    };
}

fn flushBatch(db: *common.antfly.db.DB, docs: []const common.antfly.introducer.TextDocument) !void {
    _ = try db.indexTextKernelDocuments(common.index_name, docs);
}

const IndexedDocument = struct {
    key: []const u8,
    text: []const u8,
    empty_text: bool,
    truncated: bool,
};

fn normalizedDocument(alloc: std.mem.Allocator, line: []const u8, fallback: usize, max_text_bytes: ?usize) !IndexedDocument {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{}) catch {
        const key = try std.fmt.allocPrint(alloc, "benchmark:{d}", .{fallback});
        return .{
            .key = key,
            .text = try alloc.dupe(u8, line),
            .empty_text = line.len == 0,
            .truncated = false,
        };
    };
    defer parsed.deinit();

    const key = try std.fmt.allocPrint(alloc, "benchmark:{d}", .{fallback});
    var text: []const u8 = "";

    if (parsed.value == .object) {
        if (parsed.value.object.get("text")) |text_value| {
            if (text_value == .string) text = text_value.string;
        }
    }
    const indexed_text = if (max_text_bytes) |limit| truncateUtf8(text, limit) else text;
    return .{
        .key = key,
        .text = indexed_text,
        .empty_text = indexed_text.len == 0,
        .truncated = indexed_text.len != text.len,
    };
}

fn truncateUtf8(text: []const u8, max_bytes: usize) []const u8 {
    if (text.len <= max_bytes) return text;
    var end = max_bytes;
    while (end > 0 and (text[end] & 0b1100_0000) == 0b1000_0000) : (end -= 1) {}
    return text[0..end];
}
