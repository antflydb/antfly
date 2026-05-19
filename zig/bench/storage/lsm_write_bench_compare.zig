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
    storage: []const u8 = "",
    mode: []const u8 = "",
    sample: usize = 0,
    workload: []const u8,
    ops: u64 = 0,
    logical_value_write_bytes: u64 = 0,
    ns: u64 = 0,
    ops_per_sec: f64 = 0,
    ns_per_op: f64 = 0,
    storage_write_file: u64 = 0,
    storage_write_bytes: u64 = 0,
    storage_manifest_write_file: u64 = 0,
    storage_manifest_write_bytes: u64 = 0,
    storage_rename: u64 = 0,
    storage_delete_file: u64 = 0,
    storage_delete_tree: u64 = 0,
    lsm_flushes: u64 = 0,
    lsm_flush_input_entries: u64 = 0,
    lsm_flush_output_runs: u64 = 0,
    lsm_flush_output_bytes: u64 = 0,
    lsm_flush_ns: u64 = 0,
    lsm_table_file_writes: u64 = 0,
    lsm_table_file_bytes: u64 = 0,
    lsm_table_file_logical_entry_bytes: u64 = 0,
    lsm_table_file_physical_entry_bytes: u64 = 0,
    lsm_table_file_raw_blocks: u64 = 0,
    lsm_table_file_compressed_blocks: u64 = 0,
    lsm_table_file_compression_codec_mask: u64 = 0,
    lsm_sorted_ingest_runs: u64 = 0,
    lsm_sorted_ingest_bytes: u64 = 0,
    lsm_sorted_ingest_ns: u64 = 0,
    lsm_manifest_writes: u64 = 0,
    lsm_manifest_bytes: u64 = 0,
    lsm_manifest_ns: u64 = 0,
    compactions: u64 = 0,
    compaction_input_runs: u64 = 0,
    compaction_input_bytes: u64 = 0,
    compaction_output_bytes: u64 = 0,
    compaction_ns: u64 = 0,
    runs_after: u64 = 0,
    l0_runs_after: u64 = 0,
    max_level_after: u64 = 0,
    run_bytes_after: u64 = 0,
    run_entries_after: u64 = 0,
    obsolete_paths_after: u64 = 0,
    mutable_entries_after: u64 = 0,
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
    storage_write_bytes: MetricSeries = .{},
    storage_manifest_write_bytes: MetricSeries = .{},
    storage_rename: MetricSeries = .{},
    lsm_flushes: MetricSeries = .{},
    lsm_flush_output_bytes: MetricSeries = .{},
    lsm_table_file_writes: MetricSeries = .{},
    lsm_table_file_bytes: MetricSeries = .{},
    lsm_table_file_logical_entry_bytes: MetricSeries = .{},
    lsm_table_file_physical_entry_bytes: MetricSeries = .{},
    lsm_table_file_raw_blocks: MetricSeries = .{},
    lsm_table_file_compressed_blocks: MetricSeries = .{},
    lsm_sorted_ingest_runs: MetricSeries = .{},
    lsm_sorted_ingest_bytes: MetricSeries = .{},
    lsm_sorted_ingest_ns: MetricSeries = .{},
    lsm_manifest_writes: MetricSeries = .{},
    lsm_manifest_bytes: MetricSeries = .{},
    compactions: MetricSeries = .{},
    compaction_input_bytes: MetricSeries = .{},
    compaction_output_bytes: MetricSeries = .{},
    compaction_ns: MetricSeries = .{},
    l0_runs_after: MetricSeries = .{},
    run_bytes_after: MetricSeries = .{},
    obsolete_paths_after: MetricSeries = .{},

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
        self.storage_write_bytes.deinit(allocator);
        self.storage_manifest_write_bytes.deinit(allocator);
        self.storage_rename.deinit(allocator);
        self.lsm_flushes.deinit(allocator);
        self.lsm_flush_output_bytes.deinit(allocator);
        self.lsm_table_file_writes.deinit(allocator);
        self.lsm_table_file_bytes.deinit(allocator);
        self.lsm_table_file_logical_entry_bytes.deinit(allocator);
        self.lsm_table_file_physical_entry_bytes.deinit(allocator);
        self.lsm_table_file_raw_blocks.deinit(allocator);
        self.lsm_table_file_compressed_blocks.deinit(allocator);
        self.lsm_sorted_ingest_runs.deinit(allocator);
        self.lsm_sorted_ingest_bytes.deinit(allocator);
        self.lsm_sorted_ingest_ns.deinit(allocator);
        self.lsm_manifest_writes.deinit(allocator);
        self.lsm_manifest_bytes.deinit(allocator);
        self.compactions.deinit(allocator);
        self.compaction_input_bytes.deinit(allocator);
        self.compaction_output_bytes.deinit(allocator);
        self.compaction_ns.deinit(allocator);
        self.l0_runs_after.deinit(allocator);
        self.run_bytes_after.deinit(allocator);
        self.obsolete_paths_after.deinit(allocator);
        self.* = undefined;
    }

    fn append(self: *GroupAgg, allocator: Allocator, record: Record) !void {
        self.sample_count += 1;
        try self.ns_per_op.append(allocator, record.ns_per_op);
        try self.ops_per_sec.append(allocator, record.ops_per_sec);
        try self.storage_write_bytes.append(allocator, @floatFromInt(record.storage_write_bytes));
        try self.storage_manifest_write_bytes.append(allocator, @floatFromInt(record.storage_manifest_write_bytes));
        try self.storage_rename.append(allocator, @floatFromInt(record.storage_rename));
        try self.lsm_flushes.append(allocator, @floatFromInt(record.lsm_flushes));
        try self.lsm_flush_output_bytes.append(allocator, @floatFromInt(record.lsm_flush_output_bytes));
        try self.lsm_table_file_writes.append(allocator, @floatFromInt(record.lsm_table_file_writes));
        try self.lsm_table_file_bytes.append(allocator, @floatFromInt(record.lsm_table_file_bytes));
        try self.lsm_table_file_logical_entry_bytes.append(allocator, @floatFromInt(record.lsm_table_file_logical_entry_bytes));
        try self.lsm_table_file_physical_entry_bytes.append(allocator, @floatFromInt(record.lsm_table_file_physical_entry_bytes));
        try self.lsm_table_file_raw_blocks.append(allocator, @floatFromInt(record.lsm_table_file_raw_blocks));
        try self.lsm_table_file_compressed_blocks.append(allocator, @floatFromInt(record.lsm_table_file_compressed_blocks));
        try self.lsm_sorted_ingest_runs.append(allocator, @floatFromInt(record.lsm_sorted_ingest_runs));
        try self.lsm_sorted_ingest_bytes.append(allocator, @floatFromInt(record.lsm_sorted_ingest_bytes));
        try self.lsm_sorted_ingest_ns.append(allocator, @floatFromInt(record.lsm_sorted_ingest_ns));
        try self.lsm_manifest_writes.append(allocator, @floatFromInt(record.lsm_manifest_writes));
        try self.lsm_manifest_bytes.append(allocator, @floatFromInt(record.lsm_manifest_bytes));
        try self.compactions.append(allocator, @floatFromInt(record.compactions));
        try self.compaction_input_bytes.append(allocator, @floatFromInt(record.compaction_input_bytes));
        try self.compaction_output_bytes.append(allocator, @floatFromInt(record.compaction_output_bytes));
        try self.compaction_ns.append(allocator, @floatFromInt(record.compaction_ns));
        try self.l0_runs_after.append(allocator, @floatFromInt(record.l0_runs_after));
        try self.run_bytes_after.append(allocator, @floatFromInt(record.run_bytes_after));
        try self.obsolete_paths_after.append(allocator, @floatFromInt(record.obsolete_paths_after));
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
            if (std.mem.eql(u8, group.scenario, scenario) and std.mem.eql(u8, group.workload, workload)) return index;
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

    try out.print("lsm write bench compare before={s} after={s}\n", .{ cfg.before_path, cfg.after_path });
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
    try writer.print("{s}/{s}: samples {d}->{d}\n", .{
        before.scenario,
        before.workload,
        before.sample_count,
        after.sample_count,
    });
    try printMetric(writer, allocator, "  ns/op", before.ns_per_op, after.ns_per_op);
    try printMetric(writer, allocator, "  ops/sec", before.ops_per_sec, after.ops_per_sec);
    try printMetric(writer, allocator, "  storage_write_bytes", before.storage_write_bytes, after.storage_write_bytes);
    try printMetric(writer, allocator, "  storage_manifest_write_bytes", before.storage_manifest_write_bytes, after.storage_manifest_write_bytes);
    try printMetric(writer, allocator, "  storage_rename", before.storage_rename, after.storage_rename);
    try printMetric(writer, allocator, "  lsm_flushes", before.lsm_flushes, after.lsm_flushes);
    try printMetric(writer, allocator, "  lsm_flush_output_bytes", before.lsm_flush_output_bytes, after.lsm_flush_output_bytes);
    try printMetric(writer, allocator, "  lsm_table_file_writes", before.lsm_table_file_writes, after.lsm_table_file_writes);
    try printMetric(writer, allocator, "  lsm_table_file_bytes", before.lsm_table_file_bytes, after.lsm_table_file_bytes);
    try printMetric(writer, allocator, "  lsm_table_file_logical_entry_bytes", before.lsm_table_file_logical_entry_bytes, after.lsm_table_file_logical_entry_bytes);
    try printMetric(writer, allocator, "  lsm_table_file_physical_entry_bytes", before.lsm_table_file_physical_entry_bytes, after.lsm_table_file_physical_entry_bytes);
    try printMetric(writer, allocator, "  lsm_table_file_raw_blocks", before.lsm_table_file_raw_blocks, after.lsm_table_file_raw_blocks);
    try printMetric(writer, allocator, "  lsm_table_file_compressed_blocks", before.lsm_table_file_compressed_blocks, after.lsm_table_file_compressed_blocks);
    try printMetric(writer, allocator, "  lsm_sorted_ingest_runs", before.lsm_sorted_ingest_runs, after.lsm_sorted_ingest_runs);
    try printMetric(writer, allocator, "  lsm_sorted_ingest_bytes", before.lsm_sorted_ingest_bytes, after.lsm_sorted_ingest_bytes);
    try printMetric(writer, allocator, "  lsm_sorted_ingest_ns", before.lsm_sorted_ingest_ns, after.lsm_sorted_ingest_ns);
    try printMetric(writer, allocator, "  lsm_manifest_writes", before.lsm_manifest_writes, after.lsm_manifest_writes);
    try printMetric(writer, allocator, "  lsm_manifest_bytes", before.lsm_manifest_bytes, after.lsm_manifest_bytes);
    try printMetric(writer, allocator, "  compactions", before.compactions, after.compactions);
    try printMetric(writer, allocator, "  compaction_input_bytes", before.compaction_input_bytes, after.compaction_input_bytes);
    try printMetric(writer, allocator, "  compaction_output_bytes", before.compaction_output_bytes, after.compaction_output_bytes);
    try printMetric(writer, allocator, "  compaction_ns", before.compaction_ns, after.compaction_ns);
    try printMetric(writer, allocator, "  l0_runs_after", before.l0_runs_after, after.l0_runs_after);
    try printMetric(writer, allocator, "  run_bytes_after", before.run_bytes_after, after.run_bytes_after);
    try printMetric(writer, allocator, "  obsolete_paths_after", before.obsolete_paths_after, after.obsolete_paths_after);
}

fn printMetric(writer: anytype, allocator: Allocator, label: []const u8, before: MetricSeries, after: MetricSeries) !void {
    const before_median = try before.median(allocator);
    const after_median = try after.median(allocator);
    const delta = after_median - before_median;
    const pct = if (before_median == 0)
        0
    else
        (delta / before_median) * 100.0;
    const delta_sign = if (delta >= 0) "+" else "";
    const pct_sign = if (pct >= 0) "+" else "";
    try writer.print("{s}: {d:.2} -> {d:.2} ({s}{d:.2}, {s}{d:.2}%)\n", .{
        label,
        before_median,
        after_median,
        delta_sign,
        delta,
        pct_sign,
        pct,
    });
}

fn lessThanF64(_: void, lhs: f64, rhs: f64) bool {
    return lhs < rhs;
}
