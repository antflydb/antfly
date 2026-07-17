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

pub fn main(init: std.process.Init) !void {
    // Match the allocator used by production-oriented binaries. Focused tests
    // still use std.testing.allocator/DebugAllocator for leak detection.
    const alloc = std.heap.c_allocator;

    const args = try common.parseArgs(init.minimal.args);
    common.configureBenchmarkMergePolicy(args);
    var db = try common.openDb(alloc, args.db_path);
    defer db.close();

    var stdin_buf: [64 * 1024]u8 = undefined;
    var reader = std.Io.File.stdin().readerStreaming(init.io, &stdin_buf);
    var stdout_buf: [8192]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &stdout_buf);
    defer stdout.interface.flush() catch {};

    while (try reader.interface.takeDelimiter('\n')) |line_raw| {
        // Preserve payload whitespace, including an empty ANALYZE payload.
        // Only the transport newline belongs to the protocol framing.
        const line = std.mem.trimEnd(u8, line_raw, "\r\n");
        if (line.len == 0) continue;

        const tab = std.mem.indexOfScalar(u8, line, '\t') orelse {
            try stdout.interface.writeAll("UNSUPPORTED\n");
            try stdout.interface.flush();
            continue;
        };
        const command = common.parseCommand(line[0..tab]);
        const query_text = line[tab + 1 ..];

        switch (command) {
            .unsupported => try stdout.interface.writeAll("UNSUPPORTED\n"),
            .analyze => {
                const result = try common.analyzeAlloc(alloc, query_text);
                defer common.freeAnalyzerResult(alloc, result);
                const json = try std.json.Stringify.valueAlloc(alloc, result, .{});
                defer alloc.free(json);
                try stdout.interface.writeAll(json);
                try stdout.interface.writeByte('\n');
            },
            .layout => {
                const layout = db.snapshotTextMemoryAttributionStats();
                const json = try std.json.Stringify.valueAlloc(alloc, layout, .{});
                defer alloc.free(json);
                try stdout.interface.writeAll(json);
                try stdout.interface.writeByte('\n');
            },
            .resources => {
                const resources = processResourceSnapshot();
                const json = try std.json.Stringify.valueAlloc(alloc, resources, .{});
                defer alloc.free(json);
                try stdout.interface.writeAll(json);
                try stdout.interface.writeByte('\n');
            },
            .compact => {
                const started_ns = common.antfly.platform_time.monotonicNs();
                try db.forceCompactTextIndexes();
                var layout = try db.textIndexLayoutStats(alloc, common.index_name);
                defer layout.deinit(alloc);
                const response = .{
                    .elapsed_ns = common.antfly.platform_time.monotonicNs() - started_ns,
                    .layout = layout,
                };
                const json = try std.json.Stringify.valueAlloc(alloc, response, .{});
                defer alloc.free(json);
                try stdout.interface.writeAll(json);
                try stdout.interface.writeByte('\n');
            },
            .count => {
                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const query = common.parseQuery(arena_state.allocator(), query_text, args.allow_legacy_query_syntax) catch {
                    try stdout.interface.writeAll("UNSUPPORTED\n");
                    try stdout.interface.flush();
                    continue;
                };
                const count = try common.countQuery(&db, alloc, query);
                try stdout.interface.print("{d}\n", .{count});
            },
            .explain => |external_ordinal| {
                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const query = common.parseQuery(arena_state.allocator(), query_text, args.allow_legacy_query_syntax) catch {
                    try stdout.interface.writeAll("UNSUPPORTED\n");
                    try stdout.interface.flush();
                    continue;
                };
                const stats = common.explainTerm(&db, alloc, query, external_ordinal) catch {
                    try stdout.interface.writeAll("UNSUPPORTED\n");
                    try stdout.interface.flush();
                    continue;
                };
                if (stats) |value| {
                    const json = try std.json.Stringify.valueAlloc(alloc, value, .{});
                    defer alloc.free(json);
                    try stdout.interface.writeAll(json);
                    try stdout.interface.writeByte('\n');
                } else {
                    try stdout.interface.writeAll("null\n");
                }
            },
            .top => |limit| {
                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const query = common.parseQuery(arena_state.allocator(), query_text, args.allow_legacy_query_syntax) catch {
                    try stdout.interface.writeAll("UNSUPPORTED\n");
                    try stdout.interface.flush();
                    continue;
                };
                var result = try common.topQuery(&db, alloc, query, limit, args.bm25_k1, args.bm25_b);
                defer result.deinit(alloc);
                try stdout.interface.writeAll("1\n");
            },
            .top_count => |limit| {
                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const query = common.parseQuery(arena_state.allocator(), query_text, args.allow_legacy_query_syntax) catch {
                    try stdout.interface.writeAll("UNSUPPORTED\n");
                    try stdout.interface.flush();
                    continue;
                };
                var result = try common.topQuery(&db, alloc, query, limit, args.bm25_k1, args.bm25_b);
                defer result.deinit(alloc);
                const count = try common.countQuery(&db, alloc, query);
                try stdout.interface.print("{d}\n", .{count});
            },
            .verify_top, .verify_top_count => |limit| {
                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const query = common.parseQuery(arena_state.allocator(), query_text, args.allow_legacy_query_syntax) catch {
                    try stdout.interface.writeAll("UNSUPPORTED\n");
                    try stdout.interface.flush();
                    continue;
                };
                var result = try common.topQuery(&db, alloc, query, limit, args.bm25_k1, args.bm25_b);
                defer result.deinit(alloc);
                const exact_requested = switch (command) {
                    .verify_top_count => true,
                    .verify_top => false,
                    else => unreachable,
                };
                const exact_total = if (exact_requested)
                    try common.countQuery(&db, alloc, query)
                else
                    null;
                const verification = try common.verificationResultAlloc(alloc, result, exact_total);
                defer alloc.free(verification.hits);
                const json = try std.json.Stringify.valueAlloc(alloc, verification, .{});
                defer alloc.free(json);
                try stdout.interface.writeAll(json);
                try stdout.interface.writeByte('\n');
            },
            .profile_top => |limit| {
                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const parse_started_ns = common.antfly.platform_time.monotonicNs();
                const query = common.parseQuery(arena_state.allocator(), query_text, args.allow_legacy_query_syntax) catch {
                    try stdout.interface.writeAll("UNSUPPORTED\n");
                    try stdout.interface.flush();
                    continue;
                };
                const parse_elapsed_ns = common.antfly.platform_time.monotonicNs() - parse_started_ns;
                const execute_started_ns = common.antfly.platform_time.monotonicNs();
                var result = try common.profileTopQuery(&db, alloc, query, limit, args.bm25_k1, args.bm25_b);
                const execute_elapsed_ns = common.antfly.platform_time.monotonicNs() - execute_started_ns;
                defer result.deinit(alloc);
                const verification = try common.verificationResultAlloc(alloc, result, null);
                defer alloc.free(verification.hits);
                const profile = .{
                    .schema_version = verification.schema_version,
                    .query_grammar = verification.query_grammar,
                    .total_hits = verification.total_hits,
                    .relation = verification.relation,
                    .hits = verification.hits,
                    .diagnostics = result.diagnostics,
                    .phase_ns = .{
                        .parse_and_lower = parse_elapsed_ns,
                        .execute_and_map_ordinals = execute_elapsed_ns,
                    },
                };
                const json = try std.json.Stringify.valueAlloc(alloc, profile, .{});
                defer alloc.free(json);
                try stdout.interface.writeAll(json);
                try stdout.interface.writeByte('\n');
            },
        }
        try stdout.interface.flush();
    }
}

fn timevalNs(value: anytype) u64 {
    if (value.sec < 0 or value.usec < 0) return 0;
    const seconds: u64 = @intCast(value.sec);
    const microseconds: u64 = @intCast(value.usec);
    const seconds_ns = std.math.mul(u64, seconds, std.time.ns_per_s) catch std.math.maxInt(u64);
    const microseconds_ns = std.math.mul(u64, microseconds, std.time.ns_per_us) catch std.math.maxInt(u64);
    return seconds_ns +| microseconds_ns;
}

fn processResourceSnapshot() struct {
    user_cpu_ns: u64,
    system_cpu_ns: u64,
    peak_rss_bytes: usize,
} {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    const maxrss: usize = if (usage.maxrss <= 0) 0 else @intCast(usage.maxrss);
    const peak_rss_bytes = switch (builtin.os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(usize, maxrss, 1024) catch std.math.maxInt(usize),
        else => maxrss,
    };
    return .{
        .user_cpu_ns = timevalNs(usage.utime),
        .system_cpu_ns = timevalNs(usage.stime),
        .peak_rss_bytes = peak_rss_bytes,
    };
}
