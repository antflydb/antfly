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

const builtin = @import("builtin");
const std = @import("std");
const regex = @import("antfly_regex");
const vellum = @import("antfly_vellum");

const Config = struct {
    samples: usize = 3,
    haystack_bytes: usize = 128 * 1024,
    haystack_repeats: usize = 300,
    fst_keys: usize = 20_000,
    fst_repeats: usize = 20,
};

const BenchFst = struct {
    data: []u8,
    fst: vellum.FST,

    fn deinit(self: *BenchFst, alloc: std.mem.Allocator) void {
        alloc.free(self.data);
    }
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;

    var cfg = Config{};
    try parseArgs(init.minimal.args, &cfg);

    std.debug.print("Regex Benchmark\n", .{});
    std.debug.print("================\n", .{});
    std.debug.print(
        "samples={d} haystack_bytes={d} haystack_repeats={d} fst_keys={d} fst_repeats={d}\n\n",
        .{ cfg.samples, cfg.haystack_bytes, cfg.haystack_repeats, cfg.fst_keys, cfg.fst_repeats },
    );

    try runHaystackBenchmarks(alloc, cfg);
    std.debug.print("\n", .{});
    try runFstBenchmarks(alloc, cfg);
}

fn parseArgs(args_in: std.process.Args, cfg: *Config) !void {
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--haystack-bytes")) {
            cfg.haystack_bytes = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--haystack-repeats")) {
            cfg.haystack_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--fst-keys")) {
            cfg.fst_keys = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--fst-repeats")) {
            cfg.fst_repeats = try parseNextUsize(&args, arg);
        } else {
            return error.InvalidArgument;
        }
    }
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseInt(usize, value, 10);
}

fn runHaystackBenchmarks(alloc: std.mem.Allocator, cfg: Config) !void {
    std.debug.print("Haystack\n", .{});
    std.debug.print("--------\n", .{});

    const shared_start_text = try buildTailHitHaystack(
        alloc,
        "caz00 cab00 caw00 cax00 caz11 cab11 caw11 cax11 ",
        "cap99 ",
        cfg.haystack_bytes,
    );
    defer alloc.free(shared_start_text);

    const prefix_tail_text = try buildTailHitHaystack(
        alloc,
        "cab123doz cab456doz cab789doz cab321doz ",
        "cat123dog",
        cfg.haystack_bytes,
    );
    defer alloc.free(prefix_tail_text);

    try benchHaystackCase(alloc, cfg, "shared_start_alt_tail", "car|cat|cap|can", shared_start_text);
    try benchHaystackCase(alloc, cfg, "prefix_tail", "cat.*dog", prefix_tail_text);
}

fn buildTailHitHaystack(
    alloc: std.mem.Allocator,
    miss_chunk: []const u8,
    hit_chunk: []const u8,
    target_bytes: usize,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    while (out.items.len + miss_chunk.len + hit_chunk.len < target_bytes) {
        try out.appendSlice(alloc, miss_chunk);
    }
    try out.appendSlice(alloc, hit_chunk);
    return try out.toOwnedSlice(alloc);
}

fn benchHaystackCase(
    alloc: std.mem.Allocator,
    cfg: Config,
    label: []const u8,
    pattern: []const u8,
    text: []const u8,
) !void {
    var compiled = try regex.compile(alloc, pattern);
    defer compiled.deinit();

    _ = regex.matchesCompiled(pattern, &compiled, text);

    var best_elapsed: u64 = std.math.maxInt(u64);
    var matched_any = false;
    for (0..cfg.samples) |_| {
        const start = nowNs();
        var matched_count: usize = 0;
        for (0..cfg.haystack_repeats) |_| {
            if (regex.matchesCompiled(pattern, &compiled, text)) matched_count += 1;
        }
        const elapsed = nowNs() - start;
        best_elapsed = @min(best_elapsed, elapsed);
        matched_any = matched_count > 0;
        std.mem.doNotOptimizeAway(matched_count);
    }

    printRate(label, best_elapsed, cfg.haystack_repeats, matched_any);
}

fn runFstBenchmarks(alloc: std.mem.Allocator, cfg: Config) !void {
    std.debug.print("Vellum\n", .{});
    std.debug.print("------\n", .{});

    var bench_fst = try buildBenchFst(alloc, cfg.fst_keys);
    defer bench_fst.deinit(alloc);

    try benchFstCase(alloc, cfg, "char_class_full_scan", "ca[a-z][0-9][0-9][0-9][0-9]", &bench_fst.fst, .raw);
    try benchFstCase(alloc, cfg, "grouped_alternation_raw", "(can|cap|car|cat)[0-9][0-9][0-9][0-9]", &bench_fst.fst, .raw);
    try benchFstCase(alloc, cfg, "grouped_alternation_bounded", "(can|cap|car|cat)[0-9][0-9][0-9][0-9]", &bench_fst.fst, .bounded);
}

fn buildBenchFst(alloc: std.mem.Allocator, total_keys: usize) !BenchFst {
    const prefixes = [_][]const u8{
        "caa", "cab", "cac", "cad", "cae", "caf", "cag", "cah", "cai", "caj", "cak", "cal", "cam",
        "can", "cao", "cap", "caq", "car", "cas", "cat", "cau", "cav", "caw", "cax", "cay", "caz",
    };

    var builder = try vellum.Builder.init(alloc, .{});
    defer builder.deinit();

    const base = total_keys / prefixes.len;
    const extra = total_keys % prefixes.len;
    var value: u64 = 0;

    for (prefixes, 0..) |prefix, idx| {
        const count = base + @intFromBool(idx < extra);
        for (0..count) |n| {
            var buf: [16]u8 = undefined;
            const key = try std.fmt.bufPrint(&buf, "{s}{d:0>4}", .{ prefix, n });
            try builder.insert(key, value);
            value += 1;
        }
    }

    const data = try builder.finish();
    return .{
        .data = data,
        .fst = try vellum.FST.load(data),
    };
}

const FstBenchMode = enum {
    raw,
    bounded,
};

fn benchFstCase(
    alloc: std.mem.Allocator,
    cfg: Config,
    label: []const u8,
    pattern: []const u8,
    fst: *const vellum.FST,
    mode: FstBenchMode,
) !void {
    var compiled = try regex.compile(alloc, pattern);
    defer compiled.deinit();

    const warmup_matches = switch (mode) {
        .raw => try countSearchMatches(alloc, fst, compiled.automaton()),
        .bounded => try countSearchMatchesBounded(alloc, fst, &compiled),
    };
    std.mem.doNotOptimizeAway(warmup_matches);

    var best_elapsed: u64 = std.math.maxInt(u64);
    var matches_per_search: usize = warmup_matches;
    for (0..cfg.samples) |_| {
        const start = nowNs();
        var total_matches: usize = 0;
        for (0..cfg.fst_repeats) |_| {
            total_matches += switch (mode) {
                .raw => try countSearchMatches(alloc, fst, compiled.automaton()),
                .bounded => try countSearchMatchesBounded(alloc, fst, &compiled),
            };
        }
        const elapsed = nowNs() - start;
        best_elapsed = @min(best_elapsed, elapsed);
        matches_per_search = total_matches / cfg.fst_repeats;
        std.mem.doNotOptimizeAway(total_matches);
    }

    printRateWithCount(label, best_elapsed, cfg.fst_repeats, matches_per_search);
}

fn countSearchMatches(alloc: std.mem.Allocator, fst: *const vellum.FST, aut: vellum.Automaton) !usize {
    var it = try fst.search(alloc, aut, null, null);
    defer it.deinit();

    var count: usize = 0;
    var current = it.current();
    while (current) |entry| : (current = try it.nextEntry()) {
        std.mem.doNotOptimizeAway(entry.val);
        std.mem.doNotOptimizeAway(entry.key.len);
        count += 1;
    }
    return count;
}

fn countSearchMatchesBounded(alloc: std.mem.Allocator, fst: *const vellum.FST, compiled: *regex.RegexAutomaton) !usize {
    if (compiled.prefix_literals.len == 0) {
        return countSearchMatches(alloc, fst, compiled.automaton());
    }

    var total: usize = 0;
    var end_buf: [256]u8 = undefined;
    for (compiled.prefix_literals, 0..) |prefix, idx| {
        if (prefixRangeCovered(prefix, compiled.prefix_literals, idx)) continue;

        const end = prefixRangeEnd(prefix, &end_buf);
        var it = try fst.search(alloc, compiled.automaton(), prefix, end);
        defer it.deinit();

        var current = it.current();
        while (current) |entry| : (current = try it.nextEntry()) {
            std.mem.doNotOptimizeAway(entry.val);
            std.mem.doNotOptimizeAway(entry.key.len);
            total += 1;
        }
    }
    return total;
}

fn prefixRangeCovered(prefix: []const u8, prefixes: [][]u8, current_idx: usize) bool {
    for (prefixes, 0..) |other, idx| {
        if (idx == current_idx) continue;
        if (other.len >= prefix.len) continue;
        if (std.mem.startsWith(u8, prefix, other)) return true;
    }
    return false;
}

fn prefixRangeEnd(prefix: []const u8, buf: []u8) []u8 {
    std.debug.assert(prefix.len + 1 <= buf.len);
    @memcpy(buf[0..prefix.len], prefix);
    buf[prefix.len] = 0xff;
    return buf[0 .. prefix.len + 1];
}

fn printRate(label: []const u8, best_elapsed: u64, repeats: usize, matched_any: bool) void {
    const ns_per_op = @as(f64, @floatFromInt(best_elapsed)) / @as(f64, @floatFromInt(repeats));
    const ops_per_sec = @as(f64, @floatFromInt(repeats)) / (@as(f64, @floatFromInt(best_elapsed)) / std.time.ns_per_s);
    std.debug.print(
        "{s}: {d:.1} ns/op, {d:.2} M ops/s, matched={any}\n",
        .{ label, ns_per_op, ops_per_sec / 1e6, matched_any },
    );
}

fn printRateWithCount(label: []const u8, best_elapsed: u64, repeats: usize, matches_per_search: usize) void {
    const ns_per_op = @as(f64, @floatFromInt(best_elapsed)) / @as(f64, @floatFromInt(repeats));
    const ops_per_sec = @as(f64, @floatFromInt(repeats)) / (@as(f64, @floatFromInt(best_elapsed)) / std.time.ns_per_s);
    std.debug.print(
        "{s}: {d:.1} ns/search, {d:.2} K searches/s, matches/search={d}\n",
        .{ label, ns_per_op, ops_per_sec / 1e3, matches_per_search },
    );
}

fn nowNs() u64 {
    const clock_id: std.posix.clockid_t = switch (builtin.os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => std.posix.CLOCK.UPTIME_RAW,
        else => std.posix.CLOCK.MONOTONIC,
    };
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(clock_id, &ts))) {
        .SUCCESS => return @intCast(@as(u128, @intCast(ts.sec)) * std.time.ns_per_s + @as(u128, @intCast(ts.nsec))),
        else => return 0,
    }
}
