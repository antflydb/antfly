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

const Allocator = std.mem.Allocator;

const Record = struct {
    scenario: []const u8,
    storage: []const u8,
    cache: []const u8,
    sample: usize,
    workload: []const u8,
    ops: u64,
    ns: u64,
    ops_per_sec: f64,
    ns_per_op: f64,
    latency_p50_ns: u64 = 0,
    latency_p95_ns: u64 = 0,
    latency_p99_ns: u64 = 0,
    latency_max_ns: u64 = 0,
    storage_read_file: u64,
    storage_read_range: u64,
    storage_read_trailer: u64,
    storage_file_size: u64,
    read_point_gets: u64,
    read_run_probes: u64,
    read_bloom_negatives: u64,
    read_mutable_hits: u64,
    read_l0_hits: u64,
    read_level_hits: u64,
    read_cursor_block_loads: u64 = 0,
    read_cursor_block_reuses: u64 = 0,
    read_cursor_value_borrows: u64 = 0,
    read_cursor_value_copies: u64 = 0,
    read_point_value_borrows: u64 = 0,
    read_point_value_copies: u64 = 0,
    read_table_entry_parses: u64 = 0,
    read_table_entry_parse_ns: u64 = 0,
    read_table_index_loads: u64 = 0,
    read_table_index_decodes: u64 = 0,
    read_table_block_loads: u64 = 0,
    read_table_block_bytes: u64 = 0,
    read_table_block_load_ns: u64 = 0,
    read_shared_block_cache_hits: u64 = 0,
    read_shared_block_cache_misses: u64 = 0,
    read_local_block_cache_hits: u64 = 0,
    read_local_block_cache_misses: u64 = 0,
    cache_raw_hits: u64,
    cache_raw_misses: u64,
    cache_index_hits: u64,
    cache_index_misses: u64,
    cache_block_hits: u64,
    cache_block_misses: u64,
    cache_block_waits: u64,
    cache_used_bytes_after: u64,
    cache_entries_after: u64,
};

const Config = struct {
    before_path: []const u8,
    after_path: []const u8,
};

const MetricSeries = struct {
    values: std.ArrayListUnmanaged(f64) = .empty,

    fn deinit(self: *MetricSeries, allocator: Allocator) void {
        self.values.deinit(allocator);
        self.* = .{};
    }

    fn append(self: *MetricSeries, allocator: Allocator, value: f64) !void {
        try self.values.append(allocator, value);
    }

    fn median(self: *const MetricSeries, allocator: Allocator) !f64 {
        if (self.values.items.len == 0) return 0;
        const scratch = try allocator.alloc(f64, self.values.items.len);
        defer allocator.free(scratch);
        @memcpy(scratch, self.values.items);
        std.mem.sort(f64, scratch, {}, lessThanF64);
        const mid = scratch.len / 2;
        if (scratch.len % 2 == 1) return scratch[mid];
        return (scratch[mid - 1] + scratch[mid]) / 2.0;
    }
};

const GroupAgg = struct {
    scenario: []u8,
    workload: []u8,
    sample_count: usize = 0,
    ns_per_op: MetricSeries = .{},
    ops_per_sec: MetricSeries = .{},
    storage_read_file: MetricSeries = .{},
    storage_read_range: MetricSeries = .{},
    storage_read_trailer: MetricSeries = .{},
    storage_file_size: MetricSeries = .{},
    read_run_probes: MetricSeries = .{},
    read_bloom_negatives: MetricSeries = .{},
    read_cursor_block_loads: MetricSeries = .{},
    read_cursor_block_reuses: MetricSeries = .{},
    read_cursor_value_borrows: MetricSeries = .{},
    read_cursor_value_copies: MetricSeries = .{},
    read_point_value_borrows: MetricSeries = .{},
    read_point_value_copies: MetricSeries = .{},
    read_table_entry_parses: MetricSeries = .{},
    read_table_entry_parse_ns: MetricSeries = .{},
    read_table_index_loads: MetricSeries = .{},
    read_table_index_decodes: MetricSeries = .{},
    read_table_block_loads: MetricSeries = .{},
    read_table_block_bytes: MetricSeries = .{},
    read_table_block_load_ns: MetricSeries = .{},
    read_shared_block_cache_hits: MetricSeries = .{},
    read_shared_block_cache_misses: MetricSeries = .{},
    read_local_block_cache_hits: MetricSeries = .{},
    read_local_block_cache_misses: MetricSeries = .{},
    cache_block_hit_rate: MetricSeries = .{},
    latency_p50_ns: MetricSeries = .{},
    latency_p95_ns: MetricSeries = .{},
    latency_p99_ns: MetricSeries = .{},
    latency_max_ns: MetricSeries = .{},

    fn init(allocator: Allocator, scenario: []const u8, workload: []const u8) !GroupAgg {
        return .{
            .scenario = try allocator.dupe(u8, scenario),
            .workload = try allocator.dupe(u8, workload),
        };
    }

    fn deinit(self: *GroupAgg, allocator: Allocator) void {
        allocator.free(self.scenario);
        allocator.free(self.workload);
        self.ns_per_op.deinit(allocator);
        self.ops_per_sec.deinit(allocator);
        self.storage_read_file.deinit(allocator);
        self.storage_read_range.deinit(allocator);
        self.storage_read_trailer.deinit(allocator);
        self.storage_file_size.deinit(allocator);
        self.read_run_probes.deinit(allocator);
        self.read_bloom_negatives.deinit(allocator);
        self.read_cursor_block_loads.deinit(allocator);
        self.read_cursor_block_reuses.deinit(allocator);
        self.read_cursor_value_borrows.deinit(allocator);
        self.read_cursor_value_copies.deinit(allocator);
        self.read_point_value_borrows.deinit(allocator);
        self.read_point_value_copies.deinit(allocator);
        self.read_table_entry_parses.deinit(allocator);
        self.read_table_entry_parse_ns.deinit(allocator);
        self.read_table_index_loads.deinit(allocator);
        self.read_table_index_decodes.deinit(allocator);
        self.read_table_block_loads.deinit(allocator);
        self.read_table_block_bytes.deinit(allocator);
        self.read_table_block_load_ns.deinit(allocator);
        self.read_shared_block_cache_hits.deinit(allocator);
        self.read_shared_block_cache_misses.deinit(allocator);
        self.read_local_block_cache_hits.deinit(allocator);
        self.read_local_block_cache_misses.deinit(allocator);
        self.cache_block_hit_rate.deinit(allocator);
        self.latency_p50_ns.deinit(allocator);
        self.latency_p95_ns.deinit(allocator);
        self.latency_p99_ns.deinit(allocator);
        self.latency_max_ns.deinit(allocator);
        self.* = undefined;
    }

    fn append(self: *GroupAgg, allocator: Allocator, record: Record) !void {
        self.sample_count += 1;
        try self.ns_per_op.append(allocator, record.ns_per_op);
        try self.ops_per_sec.append(allocator, record.ops_per_sec);
        try self.storage_read_file.append(allocator, @floatFromInt(record.storage_read_file));
        try self.storage_read_range.append(allocator, @floatFromInt(record.storage_read_range));
        try self.storage_read_trailer.append(allocator, @floatFromInt(record.storage_read_trailer));
        try self.storage_file_size.append(allocator, @floatFromInt(record.storage_file_size));
        try self.read_run_probes.append(allocator, @floatFromInt(record.read_run_probes));
        try self.read_bloom_negatives.append(allocator, @floatFromInt(record.read_bloom_negatives));
        try self.read_cursor_block_loads.append(allocator, @floatFromInt(record.read_cursor_block_loads));
        try self.read_cursor_block_reuses.append(allocator, @floatFromInt(record.read_cursor_block_reuses));
        try self.read_cursor_value_borrows.append(allocator, @floatFromInt(record.read_cursor_value_borrows));
        try self.read_cursor_value_copies.append(allocator, @floatFromInt(record.read_cursor_value_copies));
        try self.read_point_value_borrows.append(allocator, @floatFromInt(record.read_point_value_borrows));
        try self.read_point_value_copies.append(allocator, @floatFromInt(record.read_point_value_copies));
        try self.read_table_entry_parses.append(allocator, @floatFromInt(record.read_table_entry_parses));
        try self.read_table_entry_parse_ns.append(allocator, @floatFromInt(record.read_table_entry_parse_ns));
        try self.read_table_index_loads.append(allocator, @floatFromInt(record.read_table_index_loads));
        try self.read_table_index_decodes.append(allocator, @floatFromInt(record.read_table_index_decodes));
        try self.read_table_block_loads.append(allocator, @floatFromInt(record.read_table_block_loads));
        try self.read_table_block_bytes.append(allocator, @floatFromInt(record.read_table_block_bytes));
        try self.read_table_block_load_ns.append(allocator, @floatFromInt(record.read_table_block_load_ns));
        try self.read_shared_block_cache_hits.append(allocator, @floatFromInt(record.read_shared_block_cache_hits));
        try self.read_shared_block_cache_misses.append(allocator, @floatFromInt(record.read_shared_block_cache_misses));
        try self.read_local_block_cache_hits.append(allocator, @floatFromInt(record.read_local_block_cache_hits));
        try self.read_local_block_cache_misses.append(allocator, @floatFromInt(record.read_local_block_cache_misses));
        if (record.latency_p50_ns > 0 or record.latency_p95_ns > 0 or record.latency_p99_ns > 0 or record.latency_max_ns > 0) {
            try self.latency_p50_ns.append(allocator, @floatFromInt(record.latency_p50_ns));
            try self.latency_p95_ns.append(allocator, @floatFromInt(record.latency_p95_ns));
            try self.latency_p99_ns.append(allocator, @floatFromInt(record.latency_p99_ns));
            try self.latency_max_ns.append(allocator, @floatFromInt(record.latency_max_ns));
        }
        const block_total = record.cache_block_hits + record.cache_block_misses;
        if (block_total > 0) {
            const rate = @as(f64, @floatFromInt(record.cache_block_hits)) /
                @as(f64, @floatFromInt(block_total));
            try self.cache_block_hit_rate.append(allocator, rate);
        }
    }
};

const BenchData = struct {
    allocator: Allocator,
    groups: std.ArrayListUnmanaged(GroupAgg) = .empty,

    fn deinit(self: *BenchData) void {
        for (self.groups.items) |*group| group.deinit(self.allocator);
        self.groups.deinit(self.allocator);
        self.* = undefined;
    }

    fn loadFile(allocator: Allocator, path: []const u8) !BenchData {
        var result = BenchData{ .allocator = allocator };
        errdefer result.deinit();

        var io_impl = threadedIo();
        defer io_impl.deinit();
        const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, allocator, .limited(64 * 1024 * 1024));
        defer allocator.free(raw);

        var lines = std.mem.tokenizeScalar(u8, raw, '\n');
        while (lines.next()) |line_raw| {
            const line = std.mem.trim(u8, line_raw, " \t\r");
            if (line.len == 0 or line[0] != '{') continue;

            var parsed = try std.json.parseFromSlice(Record, allocator, line, .{
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            try result.addRecord(parsed.value);
        }
        return result;
    }

    fn addRecord(self: *BenchData, record: Record) !void {
        const maybe_index = self.findGroup(record.scenario, record.workload);
        const index = maybe_index orelse blk: {
            const group = try GroupAgg.init(self.allocator, record.scenario, record.workload);
            try self.groups.append(self.allocator, group);
            break :blk self.groups.items.len - 1;
        };
        try self.groups.items[index].append(self.allocator, record);
    }

    fn findGroup(self: *const BenchData, scenario: []const u8, workload: []const u8) ?usize {
        for (self.groups.items, 0..) |group, index| {
            if (std.mem.eql(u8, group.scenario, scenario) and std.mem.eql(u8, group.workload, workload)) {
                return index;
            }
        }
        return null;
    }
};

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;
    const cfg = try parseArgs(allocator, init.minimal.args);
    defer allocator.free(cfg.before_path);
    defer allocator.free(cfg.after_path);

    var before = try BenchData.loadFile(allocator, cfg.before_path);
    defer before.deinit();
    var after = try BenchData.loadFile(allocator, cfg.after_path);
    defer after.deinit();

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    try out.print("lsm backend bench compare before={s} after={s}\n", .{ cfg.before_path, cfg.after_path });

    for (before.groups.items) |*before_group| {
        if (after.findGroup(before_group.scenario, before_group.workload)) |after_index| {
            try printComparison(out, allocator, before_group, &after.groups.items[after_index]);
        } else {
            try out.print("{s}/{s}: only in before ({d} samples)\n", .{
                before_group.scenario,
                before_group.workload,
                before_group.sample_count,
            });
        }
    }
    for (after.groups.items) |*after_group| {
        if (before.findGroup(after_group.scenario, after_group.workload) == null) {
            try out.print("{s}/{s}: only in after ({d} samples)\n", .{
                after_group.scenario,
                after_group.workload,
                after_group.sample_count,
            });
        }
    }
    try stdout_writer.flush();
}

fn parseArgs(allocator: Allocator, proc_args: std.process.Args) !Config {
    var args = try std.process.Args.Iterator.initAllocator(proc_args, allocator);
    defer args.deinit();
    _ = args.next();

    var before_path: ?[]const u8 = null;
    var after_path: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--before")) {
            const value = args.next() orelse return error.InvalidArgument;
            before_path = try allocator.dupe(u8, value);
        } else if (std.mem.eql(u8, arg, "--after")) {
            const value = args.next() orelse return error.InvalidArgument;
            after_path = try allocator.dupe(u8, value);
        } else {
            return error.InvalidArgument;
        }
    }

    return .{
        .before_path = before_path orelse return error.InvalidArgument,
        .after_path = after_path orelse return error.InvalidArgument,
    };
}

fn printComparison(
    writer: anytype,
    allocator: Allocator,
    before: *const GroupAgg,
    after: *const GroupAgg,
) !void {
    const before_ns = try before.ns_per_op.median(allocator);
    const after_ns = try after.ns_per_op.median(allocator);
    const before_ops = try before.ops_per_sec.median(allocator);
    const after_ops = try after.ops_per_sec.median(allocator);
    const before_file = try before.storage_read_file.median(allocator);
    const after_file = try after.storage_read_file.median(allocator);
    const before_range = try before.storage_read_range.median(allocator);
    const after_range = try after.storage_read_range.median(allocator);
    const before_trailer = try before.storage_read_trailer.median(allocator);
    const after_trailer = try after.storage_read_trailer.median(allocator);
    const before_size = try before.storage_file_size.median(allocator);
    const after_size = try after.storage_file_size.median(allocator);
    const before_probes = try before.read_run_probes.median(allocator);
    const after_probes = try after.read_run_probes.median(allocator);
    const before_bloom = try before.read_bloom_negatives.median(allocator);
    const after_bloom = try after.read_bloom_negatives.median(allocator);
    const before_cursor_loads = try before.read_cursor_block_loads.median(allocator);
    const after_cursor_loads = try after.read_cursor_block_loads.median(allocator);
    const before_cursor_reuses = try before.read_cursor_block_reuses.median(allocator);
    const after_cursor_reuses = try after.read_cursor_block_reuses.median(allocator);
    const before_cursor_borrows = try before.read_cursor_value_borrows.median(allocator);
    const after_cursor_borrows = try after.read_cursor_value_borrows.median(allocator);
    const before_cursor_copies = try before.read_cursor_value_copies.median(allocator);
    const after_cursor_copies = try after.read_cursor_value_copies.median(allocator);
    const before_point_borrows = try before.read_point_value_borrows.median(allocator);
    const after_point_borrows = try after.read_point_value_borrows.median(allocator);
    const before_point_copies = try before.read_point_value_copies.median(allocator);
    const after_point_copies = try after.read_point_value_copies.median(allocator);
    const before_table_parses = try before.read_table_entry_parses.median(allocator);
    const after_table_parses = try after.read_table_entry_parses.median(allocator);
    const before_table_blocks = try before.read_table_block_loads.median(allocator);
    const after_table_blocks = try after.read_table_block_loads.median(allocator);
    const before_table_bytes = try before.read_table_block_bytes.median(allocator);
    const after_table_bytes = try after.read_table_block_bytes.median(allocator);
    const before_shared_block_hits = try before.read_shared_block_cache_hits.median(allocator);
    const after_shared_block_hits = try after.read_shared_block_cache_hits.median(allocator);
    const before_shared_block_misses = try before.read_shared_block_cache_misses.median(allocator);
    const after_shared_block_misses = try after.read_shared_block_cache_misses.median(allocator);
    const before_local_block_hits = try before.read_local_block_cache_hits.median(allocator);
    const after_local_block_hits = try after.read_local_block_cache_hits.median(allocator);
    const before_local_block_misses = try before.read_local_block_cache_misses.median(allocator);
    const after_local_block_misses = try after.read_local_block_cache_misses.median(allocator);

    try writer.print(
        "{s}/{s} ({d}->{d} samples)\n",
        .{ before.scenario, before.workload, before.sample_count, after.sample_count },
    );
    try writer.print(
        "  ns/op {d:.2} -> {d:.2} (",
        .{
            before_ns,
            after_ns,
        },
    );
    try writePercentDelta(writer, after_ns, before_ns);
    try writer.print(")  ops/s {d:.2} -> {d:.2} (", .{ before_ops, after_ops });
    try writePercentDelta(writer, after_ops, before_ops);
    try writer.writeAll(")\n");
    try writer.print(
        "  file {d:.0} -> {d:.0} (",
        .{
            before_file,
            after_file,
        },
    );
    try writePercentDelta(writer, after_file, before_file);
    try writer.print(")  range {d:.0} -> {d:.0} (", .{ before_range, after_range });
    try writePercentDelta(writer, after_range, before_range);
    try writer.print(")  trailer {d:.0} -> {d:.0} (", .{ before_trailer, after_trailer });
    try writePercentDelta(writer, after_trailer, before_trailer);
    try writer.print(")  size {d:.0} -> {d:.0} (", .{ before_size, after_size });
    try writePercentDelta(writer, after_size, before_size);
    try writer.writeAll(")\n");
    try writer.print(
        "  probes {d:.0} -> {d:.0} (",
        .{
            before_probes,
            after_probes,
        },
    );
    try writePercentDelta(writer, after_probes, before_probes);
    try writer.print(")  bloom_neg {d:.0} -> {d:.0} (", .{ before_bloom, after_bloom });
    try writePercentDelta(writer, after_bloom, before_bloom);
    try writer.print(")  cursor_loads {d:.0} -> {d:.0} (", .{ before_cursor_loads, after_cursor_loads });
    try writePercentDelta(writer, after_cursor_loads, before_cursor_loads);
    try writer.print(")  cursor_reuses {d:.0} -> {d:.0} (", .{ before_cursor_reuses, after_cursor_reuses });
    try writePercentDelta(writer, after_cursor_reuses, before_cursor_reuses);
    try writer.writeAll(")");

    const before_block_rate = try before.cache_block_hit_rate.median(allocator);
    const after_block_rate = try after.cache_block_hit_rate.median(allocator);
    if (before.cache_block_hit_rate.values.items.len > 0 or after.cache_block_hit_rate.values.items.len > 0) {
        try writer.print("  block_hit {d:.1}% -> {d:.1}% (", .{
            before_block_rate * 100.0,
            after_block_rate * 100.0,
        });
        try writePointDelta(writer, after_block_rate * 100.0, before_block_rate * 100.0);
        try writer.writeAll(")");
    }
    try writer.writeAll("\n");

    if (before_table_parses > 0 or after_table_parses > 0 or before_table_blocks > 0 or after_table_blocks > 0) {
        try writer.print("  table_parses {d:.0} -> {d:.0} (", .{ before_table_parses, after_table_parses });
        try writePercentDelta(writer, after_table_parses, before_table_parses);
        try writer.print(")  table_blocks {d:.0} -> {d:.0} (", .{ before_table_blocks, after_table_blocks });
        try writePercentDelta(writer, after_table_blocks, before_table_blocks);
        try writer.print(")  table_bytes {d:.0} -> {d:.0} (", .{ before_table_bytes, after_table_bytes });
        try writePercentDelta(writer, after_table_bytes, before_table_bytes);
        try writer.writeAll(")\n");
    }

    if (before_shared_block_hits > 0 or after_shared_block_hits > 0 or before_shared_block_misses > 0 or after_shared_block_misses > 0 or before_local_block_hits > 0 or after_local_block_hits > 0 or before_local_block_misses > 0 or after_local_block_misses > 0) {
        try writer.print("  shared_block h/m {d:.0}/{d:.0} -> {d:.0}/{d:.0}  local_block h/m {d:.0}/{d:.0} -> {d:.0}/{d:.0}\n", .{
            before_shared_block_hits,
            before_shared_block_misses,
            after_shared_block_hits,
            after_shared_block_misses,
            before_local_block_hits,
            before_local_block_misses,
            after_local_block_hits,
            after_local_block_misses,
        });
    }

    if (before_cursor_borrows > 0 or after_cursor_borrows > 0 or before_cursor_copies > 0 or after_cursor_copies > 0 or before_point_borrows > 0 or after_point_borrows > 0 or before_point_copies > 0 or after_point_copies > 0) {
        try writer.print("  cursor value borrow/copy {d:.0}/{d:.0} -> {d:.0}/{d:.0}  point value borrow/copy {d:.0}/{d:.0} -> {d:.0}/{d:.0}\n", .{
            before_cursor_borrows,
            before_cursor_copies,
            after_cursor_borrows,
            after_cursor_copies,
            before_point_borrows,
            before_point_copies,
            after_point_borrows,
            after_point_copies,
        });
    }

    if (before.latency_p95_ns.values.items.len > 0 or after.latency_p95_ns.values.items.len > 0) {
        const before_p50 = try before.latency_p50_ns.median(allocator);
        const after_p50 = try after.latency_p50_ns.median(allocator);
        const before_p95 = try before.latency_p95_ns.median(allocator);
        const after_p95 = try after.latency_p95_ns.median(allocator);
        const before_p99 = try before.latency_p99_ns.median(allocator);
        const after_p99 = try after.latency_p99_ns.median(allocator);
        const before_max = try before.latency_max_ns.median(allocator);
        const after_max = try after.latency_max_ns.median(allocator);
        try writer.print("  latency p50 {d:.0} -> {d:.0} (", .{ before_p50, after_p50 });
        try writePercentDelta(writer, after_p50, before_p50);
        try writer.print(")  p95 {d:.0} -> {d:.0} (", .{ before_p95, after_p95 });
        try writePercentDelta(writer, after_p95, before_p95);
        try writer.print(")  p99 {d:.0} -> {d:.0} (", .{ before_p99, after_p99 });
        try writePercentDelta(writer, after_p99, before_p99);
        try writer.print(")  max {d:.0} -> {d:.0} (", .{ before_max, after_max });
        try writePercentDelta(writer, after_max, before_max);
        try writer.writeAll(")\n");
    }
}

fn lessThanF64(_: void, lhs: f64, rhs: f64) bool {
    return lhs < rhs;
}

fn writePercentDelta(
    writer: anytype,
    after: f64,
    before: f64,
) !void {
    if (before == 0) {
        if (after == 0) {
            try writer.writeAll("0.0%");
        } else {
            try writer.writeAll("n/a");
        }
        return;
    }
    const pct = ((after - before) / before) * 100.0;
    if (pct >= 0) try writer.writeAll("+");
    try writer.print("{d:.1}%", .{pct});
}

fn writePointDelta(
    writer: anytype,
    after: f64,
    before: f64,
) !void {
    const delta = after - before;
    if (delta >= 0) try writer.writeAll("+");
    try writer.print("{d:.1}pp", .{delta});
}
