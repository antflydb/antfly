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
const split_storage = @import("split_storage");
const platform_time = split_storage.platform_time;

const Config = struct {
    samples: usize = 5,
    docs: usize = 20_000,
    value_size: usize = 256,
};

const DurStats = struct {
    values: std.ArrayListUnmanaged(u64) = .empty,
    total: u128 = 0,

    fn add(self: *DurStats, alloc: std.mem.Allocator, value: u64) !void {
        try self.values.append(alloc, value);
        self.total += value;
    }

    fn avg(self: DurStats) u64 {
        if (self.values.items.len == 0) return 0;
        return @intCast(self.total / self.values.items.len);
    }

    fn deinit(self: *DurStats, alloc: std.mem.Allocator) void {
        self.values.deinit(alloc);
        self.* = undefined;
    }
};

const Results = struct {
    baseline_median_ns: DurStats = .{},
    native_median_ns: DurStats = .{},
    baseline_copy_ns: DurStats = .{},
    streaming_copy_ns: DurStats = .{},

    fn deinit(self: *Results, alloc: std.mem.Allocator) void {
        self.baseline_median_ns.deinit(alloc);
        self.native_median_ns.deinit(alloc);
        self.baseline_copy_ns.deinit(alloc);
        self.streaming_copy_ns.deinit(alloc);
    }
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const cfg = try parseArgs(gpa, init.minimal.args);
    var results = Results{};
    defer results.deinit(gpa);

    var sample: usize = 0;
    while (sample < cfg.samples) : (sample += 1) {
        const run = try runSample(gpa, cfg, sample);
        try results.baseline_median_ns.add(gpa, run.baseline_median_ns);
        try results.native_median_ns.add(gpa, run.native_median_ns);
        try results.baseline_copy_ns.add(gpa, run.baseline_copy_ns);
        try results.streaming_copy_ns.add(gpa, run.streaming_copy_ns);
    }

    printStats("median_baseline", results.baseline_median_ns);
    printStats("median_native", results.native_median_ns);
    printStats("copy_baseline", results.baseline_copy_ns);
    printStats("copy_streaming", results.streaming_copy_ns);
}

fn parseArgs(alloc: std.mem.Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--value-size")) {
            cfg.value_size = try parseNextUsize(&args, arg);
        } else {
            return error.InvalidArgument;
        }
    }

    return cfg;
}

fn parseNextUsize(args: anytype, flag: []const u8) !usize {
    const value = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseUnsigned(usize, value, 10);
}

const SampleResult = struct {
    baseline_median_ns: u64,
    native_median_ns: u64,
    baseline_copy_ns: u64,
    streaming_copy_ns: u64,
};

fn runSample(alloc: std.mem.Allocator, cfg: Config, sample_idx: usize) !SampleResult {
    var src_buf: [256]u8 = undefined;
    const src_path = benchPath(&src_buf, "src", sample_idx);
    cleanupBenchDirAt(src_path);
    defer cleanupBenchDirAt(src_path);

    var src_store = try split_storage.DocStore.open(alloc, src_path, .{});
    defer src_store.close();

    try populateStore(alloc, &src_store, cfg.docs, cfg.value_size);

    var lower_buf: [64]u8 = undefined;
    const split_key = formatDocKey(&lower_buf, cfg.docs / 2);

    const median_started = nowNs();
    const baseline_median = try findMedianKeyBaseline(alloc, &src_store, "", "");
    const baseline_median_ns = elapsedSince(median_started);
    defer alloc.free(baseline_median);

    const native_started = nowNs();
    const native_median = try src_store.findMedianKey(alloc, "", "", .{ .skip_fn = &skipInternalKey });
    const native_median_ns = elapsedSince(native_started);
    defer alloc.free(native_median);

    try std.testing.expectEqualStrings(baseline_median, native_median);

    var baseline_dest_buf: [256]u8 = undefined;
    const baseline_dest_path = benchPath(&baseline_dest_buf, "baseline-dst", sample_idx);
    cleanupBenchDirAt(baseline_dest_path);
    defer cleanupBenchDirAt(baseline_dest_path);

    var baseline_dest = try split_storage.DocStore.open(alloc, baseline_dest_path, .{});
    defer baseline_dest.close();

    const baseline_copy_started = nowNs();
    try streamRangeBaseline(alloc, &src_store, split_key, "", &baseline_dest);
    const baseline_copy_ns = elapsedSince(baseline_copy_started);

    var streaming_dest_buf: [256]u8 = undefined;
    const streaming_dest_path = benchPath(&streaming_dest_buf, "streaming-dst", sample_idx);
    cleanupBenchDirAt(streaming_dest_path);
    defer cleanupBenchDirAt(streaming_dest_path);

    var streaming_dest = try split_storage.DocStore.open(alloc, streaming_dest_path, .{});
    defer streaming_dest.close();

    var mgr = try split_storage.ShardManager.init(alloc, &src_store, .{ .start = "", .end = "" });
    defer mgr.deinit();

    const streaming_copy_started = nowNs();
    try mgr.streamRange(split_key, "", &streaming_dest);
    const streaming_copy_ns = elapsedSince(streaming_copy_started);

    try expectSameVisibleKeys(alloc, &baseline_dest, &streaming_dest);

    return .{
        .baseline_median_ns = baseline_median_ns,
        .native_median_ns = native_median_ns,
        .baseline_copy_ns = baseline_copy_ns,
        .streaming_copy_ns = streaming_copy_ns,
    };
}

fn populateStore(alloc: std.mem.Allocator, store: *split_storage.DocStore, docs: usize, value_size: usize) !void {
    const batch_size = 512;
    const payload = try alloc.alloc(u8, value_size);
    defer alloc.free(payload);
    @memset(payload, 'x');

    var writes = std.ArrayListUnmanaged(split_storage.KVPair).empty;
    defer writes.deinit(alloc);
    const no_deletes: []const []const u8 = &.{};

    var key_buf: [64]u8 = undefined;
    var i: usize = 0;
    while (i < docs) : (i += 1) {
        const key = try alloc.dupe(u8, formatDocKey(&key_buf, i));
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"content\":\"{s}\",\"id\":{d}}}", .{ payload, i });
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });

        if (writes.items.len == batch_size) {
            try store.putBatch(writes.items, no_deletes);
            for (writes.items) |kv| {
                alloc.free(@constCast(kv.key));
                alloc.free(@constCast(kv.value));
            }
            writes.clearRetainingCapacity();
        }
    }

    if (writes.items.len > 0) {
        try store.putBatch(writes.items, no_deletes);
        for (writes.items) |kv| {
            alloc.free(@constCast(kv.key));
            alloc.free(@constCast(kv.value));
        }
        writes.clearRetainingCapacity();
    }

    try store.put("\x00\x00__metadata__:schema", "meta");
    try store.put("splitstate:current", "state");
    try store.put("splitdelta:0001", "delta");
}

fn findMedianKeyBaseline(alloc: std.mem.Allocator, store: *split_storage.DocStore, lower: []const u8, upper: []const u8) ![]u8 {
    const entries = try store.scanRange(alloc, lower, upper);
    defer split_storage.DocStore.freeResults(alloc, entries);

    var visible_count: usize = 0;
    for (entries) |kv| {
        if (!skipInternalKey(kv.key)) visible_count += 1;
    }
    if (visible_count == 0) return error.NotFound;

    const target = visible_count / 2;
    var index: usize = 0;
    for (entries) |kv| {
        if (skipInternalKey(kv.key)) continue;
        if (index == target) return try alloc.dupe(u8, kv.key);
        index += 1;
    }

    return error.NotFound;
}

fn streamRangeBaseline(
    alloc: std.mem.Allocator,
    src: *split_storage.DocStore,
    lower: []const u8,
    upper: []const u8,
    dest: *split_storage.DocStore,
) !void {
    const batch_size = 1000;
    const entries = try src.scanRange(alloc, lower, upper);
    defer split_storage.DocStore.freeResults(alloc, entries);

    const no_deletes: []const []const u8 = &.{};
    var batch_start: usize = 0;
    while (batch_start < entries.len) {
        const batch_end = @min(batch_start + batch_size, entries.len);
        const batch = entries[batch_start..batch_end];

        var visible_count: usize = 0;
        for (batch) |kv| {
            if (!skipInternalKey(kv.key)) visible_count += 1;
        }
        if (visible_count == 0) {
            batch_start = batch_end;
            continue;
        }

        const writes = try alloc.alloc(split_storage.KVPair, visible_count);
        defer alloc.free(writes);
        var write_idx: usize = 0;
        for (batch) |kv| {
            if (skipInternalKey(kv.key)) continue;
            writes[write_idx] = .{ .key = kv.key, .value = kv.value };
            write_idx += 1;
        }
        try dest.putBatch(writes, no_deletes);
        batch_start = batch_end;
    }
}

fn expectSameVisibleKeys(alloc: std.mem.Allocator, a: *split_storage.DocStore, b: *split_storage.DocStore) !void {
    const a_entries = try a.scanRange(alloc, "", "");
    defer split_storage.DocStore.freeResults(alloc, a_entries);
    const b_entries = try b.scanRange(alloc, "", "");
    defer split_storage.DocStore.freeResults(alloc, b_entries);

    try std.testing.expectEqual(a_entries.len, b_entries.len);
    for (a_entries, b_entries) |lhs, rhs| {
        try std.testing.expectEqualStrings(lhs.key, rhs.key);
        try std.testing.expectEqualStrings(lhs.value, rhs.value);
    }
}

fn skipInternalKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or
        std.mem.startsWith(u8, key, "splitstate:") or
        std.mem.startsWith(u8, key, "splitdelta:");
}

fn formatDocKey(buf: []u8, index: usize) []const u8 {
    return std.fmt.bufPrint(buf, "doc:{d:0>8}", .{index}) catch unreachable;
}

fn benchPath(buf: []u8, label: []const u8, sample_idx: usize) [*:0]const u8 {
    const base = "/tmp/antfly-split-bench-";
    const ts = nowNs();
    const path = std.fmt.bufPrint(buf, "{s}{s}-{d}-{d}\x00", .{ base, label, sample_idx, ts }) catch unreachable;
    return @ptrCast(path.ptr);
}

fn cleanupBenchDirAt(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn elapsedSince(started_ns: i128) u64 {
    const now = nowNs();
    return @intCast(@max(now - started_ns, 0));
}

fn printStats(label: []const u8, stats: DurStats) void {
    var sorted = std.ArrayListUnmanaged(u64).empty;
    defer sorted.deinit(std.heap.page_allocator);
    sorted.appendSlice(std.heap.page_allocator, stats.values.items) catch return;
    std.mem.sort(u64, sorted.items, {}, comptime std.sort.asc(u64));

    const median = percentileSorted(sorted.items, 50);
    const p95 = percentileSorted(sorted.items, 95);
    std.debug.print(
        "{s}: samples={d} avg={d:.3}ms median={d:.3}ms p95={d:.3}ms min={d:.3}ms max={d:.3}ms\n",
        .{
            label,
            stats.values.items.len,
            nsToMs(stats.avg()),
            nsToMs(median),
            nsToMs(p95),
            nsToMs(sorted.items[0]),
            nsToMs(sorted.items[sorted.items.len - 1]),
        },
    );
}

fn percentileSorted(sorted: []const u64, pct: usize) u64 {
    if (sorted.len == 0) return 0;
    if (sorted.len == 1) return sorted[0];
    const max_index = sorted.len - 1;
    const scaled = max_index * pct;
    const index = (scaled + 99) / 100;
    return sorted[@min(index, max_index)];
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}

fn nowNs() i128 {
    return @intCast(platform_time.monotonicNs());
}
