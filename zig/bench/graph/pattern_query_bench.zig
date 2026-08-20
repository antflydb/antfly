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
const antfly = @import("antfly_zig");

const graph_mod = antfly.graph;
const pattern_mod = antfly.graph_pattern;

const Mode = enum {
    exact,
    generic,
};

const Config = struct {
    mode: Mode = .exact,
    fanout: usize = 10_000,
    tags_per_post: usize = 8,
    target_degree: usize = 100_000,
    match_every: usize = 10,
    warmup: usize = 5,
    samples: usize = 30,
};

const PhaseAllocStats = struct {
    current_bytes: usize = 0,
    peak_bytes: usize = 0,
    total_alloc_bytes: usize = 0,
    total_free_bytes: usize = 0,
    alloc_count: usize = 0,
    free_count: usize = 0,

    fn noteAlloc(self: *PhaseAllocStats, len: usize) void {
        self.current_bytes +|= len;
        self.total_alloc_bytes +|= len;
        self.alloc_count +|= 1;
        self.peak_bytes = @max(self.peak_bytes, self.current_bytes);
    }

    fn noteFree(self: *PhaseAllocStats, len: usize) void {
        self.current_bytes -|= len;
        self.total_free_bytes +|= len;
        self.free_count +|= 1;
    }

    fn noteResize(self: *PhaseAllocStats, old_len: usize, new_len: usize) void {
        if (new_len > old_len) {
            self.noteAlloc(new_len - old_len);
        } else if (old_len > new_len) {
            self.noteFree(old_len - new_len);
        }
    }
};

const PhaseTrackingAllocator = struct {
    backing: std.mem.Allocator,
    stats: *PhaseAllocStats,

    fn allocator(self: *PhaseTrackingAllocator) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *PhaseTrackingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.stats.noteAlloc(len);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *PhaseTrackingAllocator = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.stats.noteResize(memory.len, new_len);
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *PhaseTrackingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.stats.noteResize(memory.len, new_len);
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *PhaseTrackingAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.stats.noteFree(memory.len);
    }
};

const ExactReader = struct {
    graph: *graph_mod.GraphIndex,

    pub fn getEdges(
        self: @This(),
        alloc: std.mem.Allocator,
        table: ?[]const u8,
        key: []const u8,
        edge_types: []const []const u8,
        direction: graph_mod.EdgeDirection,
    ) ![]graph_mod.Edge {
        if (table != null) return try alloc.alloc(graph_mod.Edge, 0);
        return try self.graph.getEdgesByTypes(alloc, key, edge_types, direction);
    }

    pub fn freeEdges(_: @This(), alloc: std.mem.Allocator, edges: []graph_mod.Edge) void {
        graph_mod.GraphIndex.freeEdges(alloc, edges);
    }

    pub fn probeEdges(
        self: @This(),
        alloc: std.mem.Allocator,
        table: ?[]const u8,
        probes: []const graph_mod.EdgeProbe,
    ) ![]?graph_mod.Edge {
        if (table != null) {
            const missing = try alloc.alloc(?graph_mod.Edge, probes.len);
            @memset(missing, null);
            return missing;
        }
        return try self.graph.probeEdgesAlloc(alloc, probes);
    }

    pub fn freeProbedEdges(_: @This(), alloc: std.mem.Allocator, edges: []?graph_mod.Edge) void {
        graph_mod.GraphIndex.freeProbedEdges(alloc, edges);
    }
};

/// Deliberately omits exact probes to force the semantically equivalent
/// generic expansion plan over the same graph snapshot.
const GenericReader = struct {
    graph: *graph_mod.GraphIndex,

    pub fn getEdges(
        self: @This(),
        alloc: std.mem.Allocator,
        table: ?[]const u8,
        key: []const u8,
        edge_types: []const []const u8,
        direction: graph_mod.EdgeDirection,
    ) ![]graph_mod.Edge {
        if (table != null) return try alloc.alloc(graph_mod.Edge, 0);
        return try self.graph.getEdgesByTypes(alloc, key, edge_types, direction);
    }

    pub fn freeEdges(_: @This(), alloc: std.mem.Allocator, edges: []graph_mod.Edge) void {
        graph_mod.GraphIndex.freeEdges(alloc, edges);
    }
};

const FixtureBatch = struct {
    writes: std.ArrayListUnmanaged(graph_mod.BatchWrite) = .empty,
    owned: std.ArrayListUnmanaged([]u8) = .empty,

    fn append(
        self: *FixtureBatch,
        alloc: std.mem.Allocator,
        graph: *graph_mod.GraphIndex,
        source: []const u8,
        target: []const u8,
        edge_type: []const u8,
    ) !void {
        const owned_start = self.owned.items.len;
        const source_copy = try alloc.dupe(u8, source);
        errdefer alloc.free(source_copy);
        const target_copy = try alloc.dupe(u8, target);
        errdefer alloc.free(target_copy);
        try self.owned.append(alloc, source_copy);
        errdefer self.owned.shrinkRetainingCapacity(owned_start);
        try self.owned.append(alloc, target_copy);
        try self.writes.append(alloc, .{
            .source = source_copy,
            .target = target_copy,
            .edge_type = edge_type,
        });
        if (self.writes.items.len >= 1024) try self.flush(alloc, graph);
    }

    fn flush(self: *FixtureBatch, alloc: std.mem.Allocator, graph: *graph_mod.GraphIndex) !void {
        if (self.writes.items.len == 0) return;
        try graph.batchApply(self.writes.items, &.{});
        for (self.owned.items) |bytes| alloc.free(bytes);
        self.owned.clearRetainingCapacity();
        self.writes.clearRetainingCapacity();
    }

    fn deinit(self: *FixtureBatch, alloc: std.mem.Allocator) void {
        for (self.owned.items) |bytes| alloc.free(bytes);
        self.owned.deinit(alloc);
        self.writes.deinit(alloc);
    }
};

const Sample = struct {
    elapsed_ns: u64,
    matches: usize,
    alloc_peak_bytes: usize,
    alloc_total_bytes: usize,
    stats: pattern_mod.MatchStats,
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    const cfg = try parseArgs(alloc, init.minimal.args);

    var graph = try graph_mod.GraphIndex.openWithPrivateStores(
        alloc,
        "unused-forward",
        "unused-reverse",
        "forum_graph",
        .{ .reverse_backend = .lsm_memory },
    );
    defer graph.close();
    try buildFixture(alloc, &graph, cfg);

    var warmup_index: usize = 0;
    while (warmup_index < cfg.warmup) : (warmup_index += 1) {
        var stats = pattern_mod.MatchStats{};
        const matches = try runQuery(alloc, &graph, cfg.mode, &stats);
        pattern_mod.freeMatches(alloc, matches);
    }

    const rss_before_queries = processPeakRssBytes();
    const latency_samples = try alloc.alloc(u64, cfg.samples);
    defer alloc.free(latency_samples);
    const allocation_samples = try alloc.alloc(usize, cfg.samples);
    defer alloc.free(allocation_samples);

    var last = Sample{
        .elapsed_ns = 0,
        .matches = 0,
        .alloc_peak_bytes = 0,
        .alloc_total_bytes = 0,
        .stats = .{},
    };
    for (0..cfg.samples) |sample_index| {
        last = try sampleQuery(alloc, &graph, cfg.mode);
        latency_samples[sample_index] = last.elapsed_ns;
        allocation_samples[sample_index] = last.alloc_peak_bytes;
    }
    std.mem.sort(u64, latency_samples, {}, std.sort.asc(u64));
    std.mem.sort(usize, allocation_samples, {}, std.sort.asc(usize));
    const rss_after_queries = processPeakRssBytes();

    const output = .{
        .benchmark = "graph_pattern_demand_working_set_v1",
        .mode = @tagName(cfg.mode),
        .plan = @tagName(last.stats.plan),
        .fanout = cfg.fanout,
        .tags_per_post = cfg.tags_per_post,
        .target_degree = cfg.target_degree,
        .match_every = cfg.match_every,
        .warmup = cfg.warmup,
        .samples = cfg.samples,
        .matches = last.matches,
        .latency_ns = .{
            .p50 = percentile(u64, latency_samples, 50),
            .p95 = percentile(u64, latency_samples, 95),
            .p99 = percentile(u64, latency_samples, 99),
        },
        .query_alloc_peak_bytes = .{
            .p50 = percentile(usize, allocation_samples, 50),
            .p95 = percentile(usize, allocation_samples, 95),
            .p99 = percentile(usize, allocation_samples, 99),
        },
        .last_query_alloc_total_bytes = last.alloc_total_bytes,
        .process_peak_rss_before_queries = rss_before_queries,
        .process_peak_rss_after_queries = rss_after_queries,
        .process_peak_rss_query_delta = rss_after_queries -| rss_before_queries,
        .rss_note = "process RSS is a high-water mark; use a fresh process and --warmup 0 --samples 1 for cold-query RSS",
        .work = last.stats,
    };
    const json = try std.json.Stringify.valueAlloc(alloc, output, .{});
    defer alloc.free(json);
    var stdout_buffer: [8192]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    try stdout.interface.writeAll(json);
    try stdout.interface.writeByte('\n');
    try stdout.flush();
}

fn runQuery(
    alloc: std.mem.Allocator,
    graph: *graph_mod.GraphIndex,
    mode: Mode,
    stats: *pattern_mod.MatchStats,
) ![]pattern_mod.PatternMatch {
    const contains_types = [_][]const u8{"CONTAINS"};
    const has_tag_types = [_][]const u8{"HAS_TAG"};
    const pattern = [_]pattern_mod.PatternStep{
        .{ .alias = "forum" },
        .{ .alias = "post", .edge = .{ .types = &contains_types } },
        .{ .alias = "tag", .edge = .{ .types = &has_tag_types } },
    };
    const starts = [_][]const u8{"forum:root"};
    const opts = pattern_mod.MatchOptions{
        .max_results = 0,
        .target_nodes = &.{.{ .table = null, .key = "tag:wanted" }},
        .target_required = true,
        .include_paths = false,
        .stats = stats,
        .max_explored_nodes = std.math.maxInt(usize),
        .max_explored_edges = std.math.maxInt(usize),
        .max_intermediate_states = std.math.maxInt(usize),
    };
    return switch (mode) {
        .exact => pattern_mod.matchPatternWithEdgeReader(alloc, ExactReader{ .graph = graph }, &starts, &pattern, opts),
        .generic => pattern_mod.matchPatternWithEdgeReader(alloc, GenericReader{ .graph = graph }, &starts, &pattern, opts),
    };
}

fn sampleQuery(alloc: std.mem.Allocator, graph: *graph_mod.GraphIndex, mode: Mode) !Sample {
    var alloc_stats = PhaseAllocStats{};
    var tracking = PhaseTrackingAllocator{ .backing = alloc, .stats = &alloc_stats };
    const query_alloc = tracking.allocator();
    var stats = pattern_mod.MatchStats{};
    const started_ns = antfly.platform_time.monotonicNs();
    const matches = try runQuery(query_alloc, graph, mode, &stats);
    const elapsed_ns = antfly.platform_time.monotonicNs() - started_ns;
    const match_count = matches.len;
    pattern_mod.freeMatches(query_alloc, matches);
    if (alloc_stats.current_bytes != 0) return error.BenchmarkQueryLeak;
    return .{
        .elapsed_ns = elapsed_ns,
        .matches = match_count,
        .alloc_peak_bytes = alloc_stats.peak_bytes,
        .alloc_total_bytes = alloc_stats.total_alloc_bytes,
        .stats = stats,
    };
}

fn buildFixture(alloc: std.mem.Allocator, graph: *graph_mod.GraphIndex, cfg: Config) !void {
    var batch = FixtureBatch{};
    defer batch.deinit(alloc);
    for (0..cfg.fanout) |post_index| {
        const post = try std.fmt.allocPrint(alloc, "post:{d:0>8}", .{post_index});
        defer alloc.free(post);
        try batch.append(alloc, graph, "forum:root", post, "CONTAINS");
        for (0..cfg.tags_per_post) |tag_index| {
            if (tag_index == 0 and post_index % cfg.match_every == 0) {
                try batch.append(alloc, graph, post, "tag:wanted", "HAS_TAG");
                continue;
            }
            const tag = try std.fmt.allocPrint(alloc, "tag:{d:0>4}:{d:0>8}", .{ tag_index, post_index });
            defer alloc.free(tag);
            try batch.append(alloc, graph, post, tag, "HAS_TAG");
        }
    }
    for (0..cfg.target_degree) |source_index| {
        const source = try std.fmt.allocPrint(alloc, "external:{d:0>8}", .{source_index});
        defer alloc.free(source);
        try batch.append(alloc, graph, source, "tag:wanted", "HAS_TAG");
    }
    try batch.flush(alloc, graph);
}

fn parseArgs(alloc: std.mem.Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--mode")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.mode = std.meta.stringToEnum(Mode, value) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--fanout")) {
            cfg.fanout = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--tags-per-post")) {
            cfg.tags_per_post = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--target-degree")) {
            cfg.target_degree = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--match-every")) {
            cfg.match_every = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            cfg.warmup = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.fanout == 0 or cfg.tags_per_post == 0 or cfg.match_every == 0 or cfg.samples == 0) {
        return error.InvalidArgument;
    }
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(usize, value, 10) catch {
        std.debug.print("invalid value for {s}: {s}\n", .{ flag, value });
        return error.InvalidArgument;
    };
}

fn percentile(comptime T: type, sorted: []const T, percent: usize) T {
    const rank = @min(sorted.len - 1, ((sorted.len - 1) * percent + 99) / 100);
    return sorted[rank];
}

fn processPeakRssBytes() usize {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    const maxrss: usize = if (usage.maxrss <= 0) 0 else @intCast(usage.maxrss);
    return switch (builtin.os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(usize, maxrss, 1024) catch std.math.maxInt(usize),
        else => maxrss,
    };
}
