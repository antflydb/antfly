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
const antjson = @import("antfly-json");

const Allocator = std.mem.Allocator;

const BenchConfig = struct {
    iterations: usize,
    warmup_iterations: usize,
    rounds: usize,
};

const BenchStats = struct {
    min_ns: u64,
    avg_ns: u64,
    max_ns: u64,

    fn nsPerOp(self: BenchStats, iterations: usize) f64 {
        return @as(f64, @floatFromInt(self.avg_ns)) / @as(f64, @floatFromInt(iterations));
    }
};

const ParseMode = enum {
    stdlib,
    auto,
    simd,
};

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

const SmallPayload = struct {
    name: []const u8,
    enabled: bool,
    count: u32,
    tags: []const []const u8,
};

const SearchDoc = struct {
    id: u32,
    title: []const u8,
    score: f64,
    labels: []const []const u8,
};

const SearchPayload = struct {
    provider: enum { openai, termite },
    model: []const u8,
    request_id: []const u8,
    docs: []const SearchDoc,
    options: struct {
        stream: bool,
        top_k: u32,
    },
    warnings: []const []const u8,
};

const CustomSubtree = struct {
    total: usize,
    enabled: bool,

    pub fn jsonParse(allocator: Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
        const Inner = struct {
            enabled: bool,
            items: []const struct {
                id: u32,
                score: f64,
            },
        };
        const parsed = try std.json.innerParse(Inner, allocator, source, options);
        return .{
            .total = parsed.items.len,
            .enabled = parsed.enabled,
        };
    }
};

const CustomWrappedPayload = struct {
    plain: u32,
    custom: CustomSubtree,
};

const KnownOnlyPayload = struct {
    count: u32,
    enabled: bool,
};

pub fn main() !void {
    const alloc = std.heap.page_allocator;

    const small_raw =
        \\{"name":"ada","enabled":true,"count":7,"tags":["x","y"]}
    ;
    const medium_raw = try buildSearchPayload(alloc, 32, false);
    defer alloc.free(medium_raw);
    const escaped_raw = try buildSearchPayload(alloc, 32, true);
    defer alloc.free(escaped_raw);
    const custom_subtree_raw = try buildCustomSubtreePayload(alloc, 64);
    defer alloc.free(custom_subtree_raw);
    const ignored_unknown_plain_raw = try buildIgnoredUnknownPayload(alloc, 96, false);
    defer alloc.free(ignored_unknown_plain_raw);
    const ignored_unknown_raw = try buildIgnoredUnknownPayload(alloc, 96, true);
    defer alloc.free(ignored_unknown_raw);
    const ignored_unknown_strings_raw = try buildIgnoredUnknownStringsPayload(alloc, 160);
    defer alloc.free(ignored_unknown_strings_raw);
    const ignored_unknown_numbers_raw = try buildIgnoredUnknownNumbersPayload(alloc, 220);
    defer alloc.free(ignored_unknown_numbers_raw);
    const large_value_raw = try buildSearchPayload(alloc, 160, false);
    defer alloc.free(large_value_raw);

    std.debug.print("=== JSON Bench ===\n\n", .{});
    std.debug.print("Host: {s}-{s} ({s}) simd_target_supported={}\n\n", .{
        @tagName(builtin.cpu.arch),
        @tagName(builtin.os.tag),
        @tagName(builtin.mode),
        antjson.simdTargetSupported(),
    });

    try runTypedCase(SmallPayload, "typed_small", small_raw, .{
        .iterations = 50_000,
        .warmup_iterations = 2_000,
        .rounds = 5,
    });
    try runTypedCase(SearchPayload, "typed_medium", medium_raw, .{
        .iterations = 3_000,
        .warmup_iterations = 200,
        .rounds = 5,
    });
    try runTypedCase(SearchPayload, "typed_escaped", escaped_raw, .{
        .iterations = 2_000,
        .warmup_iterations = 100,
        .rounds = 5,
    });
    try runTypedCase(CustomWrappedPayload, "typed_custom_subtree", custom_subtree_raw, .{
        .iterations = 1_200,
        .warmup_iterations = 80,
        .rounds = 5,
    });
    try runTypedCaseWithOptions(KnownOnlyPayload, "typed_ignore_unknown_escaped", ignored_unknown_raw, .{
        .ignore_unknown_fields = true,
    }, .{
        .iterations = 1_000,
        .warmup_iterations = 60,
        .rounds = 5,
    });
    try runTypedCaseWithOptions(KnownOnlyPayload, "typed_ignore_unknown_plain", ignored_unknown_plain_raw, .{
        .ignore_unknown_fields = true,
    }, .{
        .iterations = 1_000,
        .warmup_iterations = 60,
        .rounds = 5,
    });
    try runTypedCaseWithOptions(KnownOnlyPayload, "typed_ignore_unknown_strings", ignored_unknown_strings_raw, .{
        .ignore_unknown_fields = true,
    }, .{
        .iterations = 900,
        .warmup_iterations = 60,
        .rounds = 5,
    });
    try runTypedCaseWithOptions(KnownOnlyPayload, "typed_ignore_unknown_numbers", ignored_unknown_numbers_raw, .{
        .ignore_unknown_fields = true,
    }, .{
        .iterations = 900,
        .warmup_iterations = 60,
        .rounds = 5,
    });
    try runValueCase("value_large", large_value_raw, .{
        .iterations = 400,
        .warmup_iterations = 25,
        .rounds = 5,
    });

    std.debug.print("\n=== JSON Bench Complete ===\n", .{});
}

fn runTypedCase(comptime T: type, name: []const u8, raw: []const u8, cfg: BenchConfig) !void {
    return runTypedCaseWithOptions(T, name, raw, .{}, cfg);
}

fn runTypedCaseWithOptions(
    comptime T: type,
    name: []const u8,
    raw: []const u8,
    options: std.json.ParseOptions,
    cfg: BenchConfig,
) !void {
    const stdlib_stats = try measureTyped(T, .stdlib, raw, options, cfg);
    const auto_stats = try measureTyped(T, .auto, raw, options, cfg);
    const simd_stats = try measureTyped(T, .simd, raw, options, cfg);

    std.debug.print("{s} size={d} bytes\n", .{ name, raw.len });
    printTypedSelections(T, raw, options);
    printStats("stdlib", stdlib_stats, cfg.iterations, null);
    printStats("ant:auto", auto_stats, cfg.iterations, stdlib_stats);
    printStats("ant:simd", simd_stats, cfg.iterations, stdlib_stats);
    std.debug.print("\n", .{});
}

fn runValueCase(name: []const u8, raw: []const u8, cfg: BenchConfig) !void {
    const stdlib_stats = try measureValue(.stdlib, raw, cfg);
    const auto_stats = try measureValue(.auto, raw, cfg);
    const simd_stats = try measureValue(.simd, raw, cfg);

    std.debug.print("{s} size={d} bytes\n", .{ name, raw.len });
    printSelections(raw);
    printStats("stdlib", stdlib_stats, cfg.iterations, null);
    printStats("ant:auto", auto_stats, cfg.iterations, stdlib_stats);
    printStats("ant:simd", simd_stats, cfg.iterations, stdlib_stats);
    std.debug.print("\n", .{});
}

fn measureTyped(
    comptime T: type,
    mode: ParseMode,
    raw: []const u8,
    options: std.json.ParseOptions,
    cfg: BenchConfig,
) !BenchStats {
    for (0..cfg.warmup_iterations) |_| {
        try parseTyped(T, mode, raw, options);
    }

    var min_ns: u64 = std.math.maxInt(u64);
    var max_ns: u64 = 0;
    var total_ns: u128 = 0;

    for (0..cfg.rounds) |_| {
        const start_ns = nowNs();
        for (0..cfg.iterations) |_| {
            try parseTyped(T, mode, raw, options);
        }
        const elapsed = nowNs() - start_ns;
        min_ns = @min(min_ns, elapsed);
        max_ns = @max(max_ns, elapsed);
        total_ns += elapsed;
    }

    return .{
        .min_ns = min_ns,
        .avg_ns = @intCast(total_ns / cfg.rounds),
        .max_ns = max_ns,
    };
}

fn measureValue(mode: ParseMode, raw: []const u8, cfg: BenchConfig) !BenchStats {
    for (0..cfg.warmup_iterations) |_| {
        try parseValue(mode, raw);
    }

    var min_ns: u64 = std.math.maxInt(u64);
    var max_ns: u64 = 0;
    var total_ns: u128 = 0;

    for (0..cfg.rounds) |_| {
        const start_ns = nowNs();
        for (0..cfg.iterations) |_| {
            try parseValue(mode, raw);
        }
        const elapsed = nowNs() - start_ns;
        min_ns = @min(min_ns, elapsed);
        max_ns = @max(max_ns, elapsed);
        total_ns += elapsed;
    }

    return .{
        .min_ns = min_ns,
        .avg_ns = @intCast(total_ns / cfg.rounds),
        .max_ns = max_ns,
    };
}

fn parseTyped(comptime T: type, mode: ParseMode, raw: []const u8, options: std.json.ParseOptions) !void {
    switch (mode) {
        .stdlib => {
            var parsed = try std.json.parseFromSlice(T, std.heap.page_allocator, raw, options);
            defer parsed.deinit();
            std.mem.doNotOptimizeAway(parsed.value);
        },
        .auto => {
            var parsed = try antjson.parseFromSliceWithConfig(T, std.heap.page_allocator, raw, options, .{});
            defer parsed.deinit();
            std.mem.doNotOptimizeAway(parsed.value);
        },
        .simd => {
            var parsed = try antjson.parseFromSliceWithConfig(T, std.heap.page_allocator, raw, options, .{
                .preferred_backend = .simd,
                .simd_min_input_len = 0,
            });
            defer parsed.deinit();
            std.mem.doNotOptimizeAway(parsed.value);
        },
    }
}

fn parseValue(mode: ParseMode, raw: []const u8) !void {
    switch (mode) {
        .stdlib => {
            var parsed = try std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, raw, .{});
            defer parsed.deinit();
            std.mem.doNotOptimizeAway(parsed.value);
        },
        .auto => {
            var parsed = try antjson.parseFromSliceWithConfig(std.json.Value, std.heap.page_allocator, raw, .{}, .{});
            defer parsed.deinit();
            std.mem.doNotOptimizeAway(parsed.value);
        },
        .simd => {
            var parsed = try antjson.parseFromSliceWithConfig(std.json.Value, std.heap.page_allocator, raw, .{}, .{
                .preferred_backend = .simd,
                .simd_min_input_len = 0,
            });
            defer parsed.deinit();
            std.mem.doNotOptimizeAway(parsed.value);
        },
    }
}

fn printSelections(raw: []const u8) void {
    const auto_sel = antjson.backendSelectionForSlice(raw, .{});
    const simd_sel = antjson.backendSelectionForSlice(raw, .{
        .preferred_backend = .simd,
        .simd_min_input_len = 0,
    });
    std.debug.print("  selection auto={s}/{s} explicit-simd={s}/{s}\n", .{
        @tagName(auto_sel.selected),
        @tagName(auto_sel.reason),
        @tagName(simd_sel.selected),
        @tagName(simd_sel.reason),
    });
}

fn printTypedSelections(comptime T: type, raw: []const u8, options: std.json.ParseOptions) void {
    const auto_sel = antjson.backendSelectionForTypedSliceWithOptions(T, raw, options, .{});
    const simd_sel = antjson.backendSelectionForTypedSliceWithOptions(T, raw, options, .{
        .preferred_backend = .simd,
        .simd_min_input_len = 0,
    });
    std.debug.print("  selection auto={s}/{s} explicit-simd={s}/{s}\n", .{
        @tagName(auto_sel.selected),
        @tagName(auto_sel.reason),
        @tagName(simd_sel.selected),
        @tagName(simd_sel.reason),
    });
}

fn printStats(label: []const u8, stats: BenchStats, iterations: usize, baseline: ?BenchStats) void {
    const min_ns = @as(f64, @floatFromInt(stats.min_ns)) / @as(f64, @floatFromInt(iterations));
    const avg_ns = stats.nsPerOp(iterations);
    const max_ns = @as(f64, @floatFromInt(stats.max_ns)) / @as(f64, @floatFromInt(iterations));

    if (baseline) |base| {
        const speedup = base.nsPerOp(iterations) / avg_ns;
        std.debug.print("  {s: <10} min={d:.2}ns/op avg={d:.2}ns/op max={d:.2}ns/op speedup={d:.2}x\n", .{
            label,
            min_ns,
            avg_ns,
            max_ns,
            speedup,
        });
    } else {
        std.debug.print("  {s: <10} min={d:.2}ns/op avg={d:.2}ns/op max={d:.2}ns/op\n", .{
            label,
            min_ns,
            avg_ns,
            max_ns,
        });
    }
}

fn buildSearchPayload(alloc: Allocator, doc_count: usize, escaped: bool) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try list.appendSlice(alloc, "{\"provider\":\"openai\",\"model\":\"gpt-5.4\",\"request_id\":\"req-json-bench\",\"docs\":[");
    for (0..doc_count) |i| {
        if (i != 0) try list.append(alloc, ',');
        if (escaped) {
            try list.print(
                alloc,
                "{{\"id\":{d},\"title\":\"doc {d} line\\nsecond line caf\\u00E9\",\"score\":{d}.{d},\"labels\":[\"alpha\",\"esc\\t{d}\"]}}",
                .{ i, i, i % 10, i % 100, i },
            );
        } else {
            try list.print(
                alloc,
                "{{\"id\":{d},\"title\":\"doc {d} plain title for benchmark payload\",\"score\":{d}.{d},\"labels\":[\"alpha\",\"beta-{d}\"]}}",
                .{ i, i, i % 10, i % 100, i },
            );
        }
    }
    if (escaped) {
        try list.appendSlice(alloc, "],\"options\":{\"stream\":false,\"top_k\":8},\"warnings\":[\"escaped\\nwarning\",\"unicode:\\u00E9\"]}");
    } else {
        try list.appendSlice(alloc, "],\"options\":{\"stream\":false,\"top_k\":8},\"warnings\":[\"none\",\"ok\"]}");
    }

    return list.toOwnedSlice(alloc);
}

fn buildCustomSubtreePayload(alloc: Allocator, item_count: usize) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try list.appendSlice(alloc, "{\"plain\":7,\"custom\":{\"enabled\":true,\"items\":[");
    for (0..item_count) |i| {
        if (i != 0) try list.append(alloc, ',');
        try list.print(
            alloc,
            "{{\"id\":{d},\"score\":{d}.{d}}}",
            .{ i, i % 10, i % 100 },
        );
    }
    try list.appendSlice(alloc, "]}}");

    return list.toOwnedSlice(alloc);
}

fn buildIgnoredUnknownPayload(alloc: Allocator, doc_count: usize, escaped: bool) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try list.appendSlice(alloc, "{\"count\":7,\"enabled\":true,\"extra\":{\"provider\":\"openai\",\"docs\":[");
    for (0..doc_count) |i| {
        if (i != 0) try list.append(alloc, ',');
        if (escaped) {
            try list.print(
                alloc,
                "{{\"id\":{d},\"title\":\"ignored {d} line\\nvalue\",\"score\":{d}.{d},\"labels\":[\"alpha\",\"skip\\t{d}\"]}}",
                .{ i, i, i % 10, i % 100, i },
            );
        } else {
            try list.print(
                alloc,
                "{{\"id\":{d},\"title\":\"ignored {d} plain payload\",\"score\":{d}.{d},\"labels\":[\"alpha\",\"beta-{d}\"]}}",
                .{ i, i, i % 10, i % 100, i },
            );
        }
    }
    try list.appendSlice(alloc, "],\"meta\":{\"nested\":{\"ok\":true,\"count\":42}},\"warnings\":[\"a\",\"b\"]}}");

    return list.toOwnedSlice(alloc);
}

fn buildIgnoredUnknownStringsPayload(alloc: Allocator, item_count: usize) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try list.appendSlice(alloc, "{\"count\":7,\"enabled\":true,\"extra\":{\"items\":[");
    for (0..item_count) |i| {
        if (i != 0) try list.append(alloc, ',');
        try list.print(
            alloc,
            "{{\"title\":\"ignored title {d}\",\"body\":\"plain string payload {d}\",\"labels\":[\"alpha\",\"beta\",\"gamma\"]}}",
            .{ i, i },
        );
    }
    try list.appendSlice(alloc, "],\"warnings\":[\"a\",\"b\",\"c\"]}}");

    return list.toOwnedSlice(alloc);
}

fn buildIgnoredUnknownNumbersPayload(alloc: Allocator, item_count: usize) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try list.appendSlice(alloc, "{\"count\":7,\"enabled\":true,\"extra\":{\"series\":[");
    for (0..item_count) |i| {
        if (i != 0) try list.append(alloc, ',');
        try list.print(
            alloc,
            "{{\"id\":{d},\"score\":{d}.{d},\"values\":[{d},{d},{d},{d}]}}",
            .{ i, i % 10, i % 100, i, i + 1, i + 2, i + 3 },
        );
    }
    try list.appendSlice(alloc, "]}}");

    return list.toOwnedSlice(alloc);
}
