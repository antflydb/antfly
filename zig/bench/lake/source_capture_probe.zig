// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Source capture comparison. Every callback token and encoded byte is
//! attested against the independent materialized path across alternating runs.
const std = @import("std");
const local = @import("local");
fn now(clock: std.c.clockid_t) i96 {
    var stamp: std.c.timespec = undefined;
    if (std.c.clock_gettime(clock, &stamp) != 0) @panic("clock unavailable");
    return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
}
test "lake streaming source capture probe" {
    const repeats: usize = if (std.c.getenv("ANTFLY_CAPTURE_REPEATS")) |s| try std.fmt.parseInt(usize, std.mem.span(s), 10) else 65536;
    if (repeats == 0) return error.InvalidBenchmarkShape;
    const pattern = "Needle repeated token café ";
    const bindings: usize = if (std.c.getenv("ANTFLY_CAPTURE_BINDINGS")) |value| try std.fmt.parseInt(usize, std.mem.span(value), 10) else 1;
    if (bindings == 0 or bindings > 4) return error.InvalidBenchmarkShape;
    const diverse = std.c.getenv("ANTFLY_CAPTURE_DIVERSE") != null;
    var generated: std.ArrayList(u8) = .empty;
    defer generated.deinit(std.heap.page_allocator);
    for (0..repeats) |i| {
        var buffer: [128]u8 = undefined;
        const bytes = if (diverse) try std.fmt.bufPrint(&buffer, "Needle{0d:0>6} repeated{0d:0>6} token{0d:0>6} café{0d:0>6} ", .{i % 1024}) else pattern;
        try generated.appendSlice(std.heap.page_allocator, bytes);
    }
    const input = generated.items;
    const legacy = std.c.getenv("ANTFLY_MATERIALIZED_CAPTURE") != null;
    var budget: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = if (std.c.getenv("ANTFLY_CAPTURE_QUOTA")) |value| try std.fmt.parseInt(usize, std.mem.span(value), 10) else 256 << 20 };
    defer std.debug.assert(budget.live == 0);
    const Capture = local.section_source_capture.Capture;
    const analyzer = if (std.c.getenv("ANTFLY_CAPTURE_ENGLISH") != null) &local.search_analysis.default_analyzer else &local.search_analysis.simple_analyzer;
    if (std.c.getenv("ANTFLY_CAPTURE_INDEX") != null) {
        if (legacy) return error.InvalidBenchmarkShape;
        return buildIndex(input, repeats, bindings, analyzer, &budget);
    }
    const signature = try local.section_source_column.fingerprint(std.heap.page_allocator, analyzer);
    const Observer = struct {
        count: u32 = 0,
        hash: std.hash.Wyhash = .init(0),
        fn accept(raw: *anyopaque, token: local.search_analysis.Token) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.count, token.position);
            self.hash.update(token.term);
            var bounds: [8]u8 = undefined;
            std.mem.writeInt(u32, bounds[0..4], token.start_byte, .little);
            std.mem.writeInt(u32, bounds[4..8], token.end_byte, .little);
            self.hash.update(&bounds);
            self.count += 1;
        }
    };
    var observers: [4]Observer = @splat(.{});
    var replay_reads: ?usize = null;
    var group_buffers: ?usize = null;
    const cpu = now(.THREAD_CPUTIME_ID);
    const wall = now(.MONOTONIC);
    var capture = Capture.init(budget.allocator(), true, std.testing.io, ".", null);
    defer capture.deinit();
    const request: Capture.Request = .{ .key = .{ .doc = 0, .field = "body" }, .signature = signature, .text_bytes = input.len };
    if (legacy) {
        var owner = std.heap.ArenaAllocator.init(budget.allocator());
        defer owner.deinit();
        const tokens = try analyzer.analyzeWithSourceOffsets(owner.allocator(), input);
        for (0..bindings) |binding| {
            for (tokens) |token| try Observer.accept(&observers[binding], token);
            if (binding == 0) try capture.record(request, tokens);
        }
    } else if (bindings == 1) {
        var stream = analyzer.sourceStream(budget.allocator(), input).?;
        defer stream.deinit();
        const count = try capture.recordStream(request, &stream, .{ .ptr = &observers[0], .accept = Observer.accept });
        try std.testing.expectEqual(observers[0].count, count);
    } else {
        const Cache = local.section_analysis_reuse.Cache;
        var cache = Cache.init(budget.allocator());
        defer cache.deinit();
        const key: Cache.Key = .{ .field = "body", .text = input, .signature = signature };
        for (0..bindings) |_| try cache.note(key);
        for (0..bindings) |binding| {
            const lease = (try cache.acquireReplay(key, analyzer, std.testing.io, ".", null)).?;
            defer lease.release();
            const tokens = try lease.visit(Capture.TokenConsumer{ .ptr = &observers[binding], .accept = Observer.accept });
            if (binding == 0) try capture.recordReplay(request, tokens);
            const spool = lease.entry.replay.?;
            group_buffers = spool.group_buffer_allocations;
            replay_reads = spool.grouping_read_calls + if (spool.run) |run| run.read_calls else @as(usize, 0);
        }
        try std.testing.expectEqual(@as(usize, 1), cache.analyses);
        try std.testing.expectEqual(bindings - 1, cache.reuses);
    }
    const elapsed = now(.THREAD_CPUTIME_ID) - cpu;
    const elapsed_wall = now(.MONOTONIC) - wall;
    const peak = budget.peak;
    const observed = &observers[0];
    for (observers[0..bindings]) |*observer| {
        try std.testing.expectEqual(repeats * 4, observer.count);
        try std.testing.expectEqual(observed.hash.final(), observer.hash.final());
    }
    const variants = try capture.variants(0, "body");
    try std.testing.expectEqual(@as(usize, 1), variants.items.len);
    const entry = variants.items[0];
    const encoded = try std.heap.page_allocator.alloc(u8, entry.bytes);
    defer std.heap.page_allocator.free(encoded);
    try variants.view.readInto(entry.offset, encoded);
    var input_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(input, &input_digest, .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
    std.debug.print("CAPTURE legacy={} bindings={d} diverse={} input={d} tokens={d} cpu_ns={d} wall_ns={d} peak={d} quota={d} replay_reads={?d} group_buffers={?d} token_hash={x} encoded_sha256={s} input_sha256={s}\n", .{ legacy, bindings, diverse, input.len, observed.count, elapsed, elapsed_wall, peak, budget.limit, replay_reads, group_buffers, observed.hash.final(), std.fmt.bytesToHex(digest, .lower), std.fmt.bytesToHex(input_digest, .lower) });
}

/// A complete native segment build from typed source. Immutable input setup,
/// result hashing and independent search verification are outside timing.
fn buildIndex(input: []const u8, repeats: usize, bindings: usize, analyzer: *const local.search_analysis.Analyzer, budget: *local.sql_memory_budget) !void {
    const a = std.heap.page_allocator;
    var root: std.json.ObjectMap = .empty;
    defer root.deinit(a);
    try root.put(a, "body", .{ .string = input });
    const names = [_][]const u8{ "body", "_all", "named_one", "named_two" };
    var fields: [4]local.introducer.TextField = undefined;
    for (fields[0..bindings], names[0..bindings]) |*field, name| field.* = .{ .field_name = name, .source_field = "body", .text = input };
    var profile: local.introducer.BuildTextProfile = .{};
    const cpu = now(.THREAD_CPUTIME_ID);
    const wall = now(.MONOTONIC);
    const bytes = try local.introducer.buildSegmentFromTextWithAnalysisOptions(budget.allocator(), &.{.{ .id = "row", .stored_data = "{}", .stored_source = .{ .object = root }, .text_fields = fields[0..bindings] }}, analyzer, .{}, .{ .store_source_columns = true, .profile = &profile, .postings_run_io = std.testing.io });
    defer budget.allocator().free(bytes);
    const elapsed = now(.THREAD_CPUTIME_ID) - cpu;
    const elapsed_wall = now(.MONOTONIC) - wall;
    const peak = budget.peak;
    try std.testing.expectEqual(@as(u64, @intCast(repeats * 4 * bindings)), profile.token_count);
    if (bindings > 1) {
        try std.testing.expectEqual(@as(u64, 1), profile.shared_analysis_runs);
        try std.testing.expectEqual(@as(u64, @intCast(bindings - 1)), profile.shared_analysis_reuses);
    }
    var writer = try local.index.IndexWriter.init(a);
    defer writer.deinit();
    try writer.addSegment(bytes);
    var stream = analyzer.sourceStream(a, input).?;
    defer stream.deinit();
    const token = (try stream.next()).?;
    const found = try writer.snapshot().search(a, names[bindings - 1], &.{token.term}, 1);
    defer a.free(found.hits);
    try std.testing.expectEqual(@as(usize, 1), found.hits.len);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    var input_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(input, &input_digest, .{});
    std.debug.print("INDEX bindings={d} input={d} tokens={d} cpu_ns={d} wall_ns={d} peak={d} quota={d} bytes={d} analyses={d} reuses={d} encoded_sha256={s} input_sha256={s}\n", .{ bindings, input.len, profile.token_count, elapsed, elapsed_wall, peak, budget.limit, bytes.len, profile.shared_analysis_runs, profile.shared_analysis_reuses, std.fmt.bytesToHex(digest, .lower), std.fmt.bytesToHex(input_digest, .lower) });
}
