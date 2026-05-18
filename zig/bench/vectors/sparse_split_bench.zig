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
const platform_time = antfly.platform_time;

const sparse = antfly.sparse;

const BenchConfig = struct {
    docs: usize = 512,
    terms_per_doc: usize = 8,
    term_space: u32 = 128,
    samples: usize = 5,
    chunk_size: usize = 64,
    seed: u64 = 42,
    child_no_sync: bool = false,
};

const PhaseStats = struct {
    total: u64 = 0,
    min: u64 = std.math.maxInt(u64),
    max: u64 = 0,

    fn add(self: *PhaseStats, value: u64) void {
        self.total += value;
        self.min = @min(self.min, value);
        self.max = @max(self.max, value);
    }

    fn avg(self: PhaseStats, samples: usize) u64 {
        return if (samples == 0) 0 else @divTrunc(self.total, samples);
    }
};

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    const cfg = try parseArgs(init.minimal.args);

    var source_path = TestPath{};
    const source_dir = source_path.init("src");
    defer source_path.cleanup();

    var src = try sparse.SparseIndex.open(alloc, source_dir, .{
        .chunk_size = @intCast(cfg.chunk_size),
    });
    defer src.close();

    const source_build_ns = try buildSource(alloc, &src, cfg);

    var split_key_buf: [32]u8 = undefined;
    const split_key = try std.fmt.bufPrint(&split_key_buf, "doc:{d:0>8}", .{cfg.docs / 2});

    const planning_started = nanotime();
    const planning = try src.splitPlanningStats(alloc, split_key, "");
    const planning_ns = nanotime() - planning_started;

    var total_stats = PhaseStats{};
    var select_stats = PhaseStats{};
    var terms_stats = PhaseStats{};
    var commit_stats = PhaseStats{};
    for (0..cfg.samples) |sample| {
        const result = try benchChildHandoff(alloc, cfg, &src, split_key, sample);
        total_stats.add(result.total_ns);
        select_stats.add(result.select_docs_ns);
        terms_stats.add(result.terms_ns);
        commit_stats.add(result.commit_ns);
    }

    std.debug.print(
        "Sparse split bench docs={d} terms_per_doc={d} term_space={d} chunk_size={d} samples={d} child_no_sync={}\n",
        .{ cfg.docs, cfg.terms_per_doc, cfg.term_space, cfg.chunk_size, cfg.samples, cfg.child_no_sync },
    );
    std.debug.print(
        "source_build={d:.3}ms split_plan={d:.3}ms selected_docs={d} touched_terms={d} right_only_chunks={d} mixed_chunks={d} right_only_postings={d} mixed_right_postings={d}\n",
        .{
            nsMs(source_build_ns),
            nsMs(planning_ns),
            planning.selected_docs,
            planning.touched_terms,
            planning.right_only_chunks,
            planning.mixed_chunks,
            planning.right_only_postings,
            planning.mixed_right_postings,
        },
    );
    printPhase("child_sparse_handoff", total_stats, cfg.samples);
    printPhase("  select_docs", select_stats, cfg.samples);
    printPhase("  terms", terms_stats, cfg.samples);
    printPhase("  commit", commit_stats, cfg.samples);
}

const HandoffSample = struct {
    total_ns: u64,
    select_docs_ns: u64,
    terms_ns: u64,
    commit_ns: u64,
};

fn parseArgs(args_in: std.process.Args) !BenchConfig {
    var cfg = BenchConfig{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--terms-per-doc")) {
            cfg.terms_per_doc = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--term-space")) {
            cfg.term_space = @intCast(try parseNextUsize(&args));
        } else if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--chunk-size")) {
            cfg.chunk_size = try parseNextUsize(&args);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try parseNextU64(&args);
        } else if (std.mem.eql(u8, arg, "--child-no-sync")) {
            cfg.child_no_sync = true;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.terms_per_doc == 0 or cfg.term_space == 0 or cfg.samples == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator) !usize {
    const raw = args.next() orelse return error.InvalidArgument;
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseNextU64(args: *std.process.Args.Iterator) !u64 {
    const raw = args.next() orelse return error.InvalidArgument;
    return try std.fmt.parseInt(u64, raw, 10);
}

fn buildSource(alloc: std.mem.Allocator, src: *sparse.SparseIndex, cfg: BenchConfig) !u64 {
    var rng = std.Random.DefaultPrng.init(cfg.seed);
    const random = rng.random();

    const writes = try alloc.alloc(sparse.SparseWrite, cfg.docs);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.doc_id));
            alloc.free(@constCast(write.vec.indices));
            alloc.free(@constCast(write.vec.values));
        }
        alloc.free(writes);
    }

    for (0..cfg.docs) |i| {
        const doc_id = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i});
        const indices = try alloc.alloc(u32, cfg.terms_per_doc);
        const values = try alloc.alloc(f32, cfg.terms_per_doc);
        var base_term: u32 = @intCast((i * 3) % cfg.term_space);
        for (0..cfg.terms_per_doc) |j| {
            indices[j] = (base_term + @as(u32, @intCast(j * 7))) % cfg.term_space;
            values[j] = 0.1 + random.float(f32) * 0.9;
        }
        std.mem.sort(u32, indices, {}, std.sort.asc(u32));
        writes[i] = .{
            .doc_id = doc_id,
            .vec = .{
                .indices = indices,
                .values = values,
            },
        };
        base_term += 1;
    }

    const started = nanotime();
    try src.batch(writes, &.{});
    return nanotime() - started;
}

fn benchChildHandoff(
    alloc: std.mem.Allocator,
    cfg: BenchConfig,
    src: *sparse.SparseIndex,
    split_key: []const u8,
    sample: usize,
) !HandoffSample {
    var dest_path = TestPath{};
    const dest_dir = dest_path.initChild("dst", sample);
    defer dest_path.cleanup();

    var dest = try sparse.SparseIndex.open(alloc, dest_dir, .{
        .chunk_size = @intCast(cfg.chunk_size),
        .no_sync = cfg.child_no_sync,
    });
    defer dest.close();

    const started = nanotime();
    var rebuilt = try src.handoffRangeInto(&dest, alloc, split_key, "", false);
    defer rebuilt.deinit(alloc);
    const total_ns = nanotime() - started;
    return .{
        .total_ns = total_ns,
        .select_docs_ns = rebuilt.select_docs_ns,
        .terms_ns = rebuilt.terms_ns,
        .commit_ns = rebuilt.commit_ns,
    };
}

fn printPhase(label: []const u8, stats: PhaseStats, samples: usize) void {
    std.debug.print(
        "{s}: avg={d:.3}ms min={d:.3}ms max={d:.3}ms\n",
        .{
            label,
            nsMs(stats.avg(samples)),
            nsMs(if (stats.min == std.math.maxInt(u64)) 0 else stats.min),
            nsMs(stats.max),
        },
    );
}

fn nsMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e6;
}

const TestPath = struct {
    buf: [256]u8 = undefined,

    fn init(self: *TestPath, label: []const u8) [*:0]const u8 {
        const ts = nanotime();
        const slice = std.fmt.bufPrint(&self.buf, "/tmp/antfly-sparse-split-{s}-{d}\x00", .{ label, ts }) catch unreachable;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
        return @ptrCast(slice.ptr);
    }

    fn initChild(self: *TestPath, label: []const u8, sample: usize) [*:0]const u8 {
        const ts = nanotime();
        const slice = std.fmt.bufPrint(&self.buf, "/tmp/antfly-sparse-split-{s}-{d}-{d}\x00", .{ label, sample, ts }) catch unreachable;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
        return @ptrCast(slice.ptr);
    }

    fn cleanup(self: *TestPath) void {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(&self.buf)))) catch {};
    }
};

fn nanotime() u64 {
    return platform_time.monotonicNs();
}
