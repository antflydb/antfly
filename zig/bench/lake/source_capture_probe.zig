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
    const input = try std.heap.page_allocator.alloc(u8, try std.math.mul(usize, repeats, pattern.len));
    defer std.heap.page_allocator.free(input);
    for (0..repeats) |i| @memcpy(input[i * pattern.len ..][0..pattern.len], pattern);
    const legacy = std.c.getenv("ANTFLY_MATERIALIZED_CAPTURE") != null;
    var budget: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = 256 << 20 };
    defer std.debug.assert(budget.live == 0);
    const Capture = local.section_source_capture.Capture;
    const analyzer = if (std.c.getenv("ANTFLY_CAPTURE_ENGLISH") != null) &local.search_analysis.default_analyzer else &local.search_analysis.simple_analyzer;
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
    var observed: Observer = .{};
    const cpu = now(.THREAD_CPUTIME_ID);
    const wall = now(.MONOTONIC);
    var capture = Capture.init(budget.allocator(), true, std.testing.io, ".", null);
    defer capture.deinit();
    const request: Capture.Request = .{ .key = .{ .doc = 0, .field = "body" }, .signature = signature, .text_bytes = input.len };
    if (legacy) {
        var owner = std.heap.ArenaAllocator.init(budget.allocator());
        defer owner.deinit();
        const tokens = try analyzer.analyzeWithSourceOffsets(owner.allocator(), input);
        for (tokens) |token| try Observer.accept(&observed, token);
        try capture.record(request, tokens);
    } else {
        var stream = analyzer.sourceStream(budget.allocator(), input).?;
        defer stream.deinit();
        try std.testing.expectEqual(observed.count, @as(u32, 0));
        const count = try capture.recordStream(request, &stream, .{ .ptr = &observed, .accept = Observer.accept });
        try std.testing.expectEqual(observed.count, count);
    }
    const elapsed = now(.THREAD_CPUTIME_ID) - cpu;
    const elapsed_wall = now(.MONOTONIC) - wall;
    const peak = budget.peak;
    try std.testing.expectEqual(repeats * 4, observed.count);
    const variants = try capture.variants(0, "body");
    try std.testing.expectEqual(@as(usize, 1), variants.items.len);
    const entry = variants.items[0];
    const encoded = try std.heap.page_allocator.alloc(u8, entry.bytes);
    defer std.heap.page_allocator.free(encoded);
    try variants.view.readInto(entry.offset, encoded);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
    std.debug.print("CAPTURE legacy={} input={d} tokens={d} cpu_ns={d} wall_ns={d} peak={d} quota={d} token_hash={x} encoded_sha256={s}\n", .{ legacy, input.len, observed.count, elapsed, elapsed_wall, peak, budget.limit, observed.hash.final(), std.fmt.bytesToHex(digest, .lower) });
}
