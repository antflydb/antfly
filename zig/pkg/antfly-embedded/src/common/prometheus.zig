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

const std = @import("std");
const testing = std.testing;

/// Writes a Prometheus text-format metric (HELP, TYPE, value) for a single
/// scalar counter/gauge. Matches the format used by Antfly inference.
pub fn appendPromMetric(
    writer: *std.Io.Writer,
    name: []const u8,
    metric_type: []const u8,
    help: []const u8,
    value: u64,
) !void {
    try appendPromMetricHeader(writer, name, metric_type, help);
    try appendPromSample(writer, name, value);
}

pub const PromLabel = struct {
    name: []const u8,
    value: []const u8,
};

/// Shared by data and standalone health endpoints. Inputs are process-owned
/// cache snapshots, not request traffic attribution. Reasons are static error
/// names; no credential, filesystem path, or per-query labels are exported.
pub fn appendLakeCacheMetrics(writer: *std.Io.Writer, ranges: anytype, disk: anytype) !void {
    try appendPromMetric(writer, "antfly_lake_cache_disk_ready", "gauge", "Persistent lake cache owner is initialized", @intFromBool(disk != null));
    try appendPromMetric(writer, "antfly_lake_cache_disk_unavailable", "gauge", "Optional persistent lake cache initialization failed", @intFromBool(ranges.disk_unavailable != null));
    if (ranges.disk_unavailable) |reason| try appendPromMetricLabeled(writer, "antfly_lake_cache_disk_unavailable_info", "gauge", "Latest persistent cache startup error", &.{.{ .name = "reason", .value = reason }}, 1);
    inline for (.{
        .{ "disk_init_attempts_total", "disk_init_attempts" },
        .{ "disk_init_failures_total", "disk_init_failures" },
        .{ "provider_reads_total", "provider_reads" },
        .{ "provider_bytes_total", "provider_bytes" },
        .{ "disk_hits_total", "disk_hits" },
        .{ "disk_bytes_total", "disk_bytes" },
        .{ "completed_queries_total", "completed_queries" },
    }) |metric| try appendPromMetric(writer, "antfly_lake_cache_" ++ metric[0], "counter", "Lake cache " ++ metric[1], @intCast(@field(ranges, metric[1])));
    try appendPromMetricHeader(writer, "antfly_lake_query_phase_nanoseconds_total", "counter", "Completed indexed lake search time by phase; hydration is nested in search and delivery");
    inline for (.{ "total", "publication", "search", "hydration", "delivery" }) |phase| try appendPromSampleLabeled(writer, "antfly_lake_query_phase_nanoseconds_total", &.{.{ .name = "phase", .value = phase }}, @field(ranges, "query_" ++ phase ++ "_ns"));
    const Disk = @typeInfo(@TypeOf(disk)).optional.child;
    const stats = disk orelse Disk{};
    inline for (.{ "read_hits", "read_misses", "read_errors", "writes_completed", "write_errors", "writes_dropped", "writes_coalesced", "dropped_bytes", "corrupt_entries_removed", "evicted_entries", "evicted_bytes" }) |field| try appendPromMetric(writer, "antfly_lake_disk_cache_" ++ field ++ "_total", "counter", "Persistent lake cache " ++ field, @intCast(@field(stats, field)));
    inline for (.{ "queued_entries", "queued_bytes", "entries", "stored_bytes" }) |field| try appendPromMetric(writer, "antfly_lake_disk_cache_" ++ field, "gauge", "Persistent lake cache " ++ field, @intCast(@field(stats, field)));
    if (stats.last_write_error) |reason| try appendPromMetricLabeled(writer, "antfly_lake_disk_cache_last_write_error_info", "gauge", "Most recent persistent cache worker error since process start", &.{.{ .name = "reason", .value = reason }}, 1);
}

test "external lake cache metrics expose errors traffic and nested phases with bounded labels" {
    const ranges = .{ .disk_unavailable = @as(?[]const u8, "NotDir"), .disk_init_attempts = @as(u64, 2), .disk_init_failures = @as(u64, 1), .provider_reads = @as(u64, 4), .provider_bytes = @as(u64, 1024), .disk_hits = @as(u64, 3), .disk_bytes = @as(u64, 512), .completed_queries = @as(u64, 1), .query_total_ns = @as(u64, 100), .query_publication_ns = @as(u64, 20), .query_search_ns = @as(u64, 50), .query_hydration_ns = @as(u64, 30), .query_delivery_ns = @as(u64, 30) };
    const Disk = struct {
        read_hits: u64 = 0,
        read_misses: u64 = 0,
        read_errors: u64 = 0,
        writes_completed: u64 = 0,
        write_errors: u64 = 0,
        writes_dropped: u64 = 0,
        writes_coalesced: u64 = 0,
        dropped_bytes: u64 = 0,
        corrupt_entries_removed: u64 = 0,
        evicted_entries: u64 = 0,
        evicted_bytes: u64 = 0,
        queued_entries: u64 = 0,
        queued_bytes: u64 = 0,
        entries: u64 = 0,
        stored_bytes: u64 = 0,
        last_write_error: ?[]const u8 = null,
    };
    var buffer: [16384]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    try appendLakeCacheMetrics(&writer, ranges, @as(?Disk, null));
    try testing.expect(std.mem.indexOf(u8, writer.buffered(), "antfly_lake_cache_disk_ready 0\n") != null);
    try testing.expect(std.mem.indexOf(u8, writer.buffered(), "reason=\"NotDir\"") != null);
    try testing.expect(std.mem.indexOf(u8, writer.buffered(), "phase=\"hydration\"} 30\n") != null);
    writer = .fixed(&buffer);
    try appendLakeCacheMetrics(&writer, ranges, @as(?Disk, .{ .writes_completed = 7, .write_errors = 1, .writes_dropped = 2, .last_write_error = "AccessDenied" }));
    try testing.expect(std.mem.indexOf(u8, writer.buffered(), "antfly_lake_disk_cache_writes_completed_total 7\n") != null);
    try testing.expect(std.mem.indexOf(u8, writer.buffered(), "antfly_lake_disk_cache_writes_dropped_total 2\n") != null);
    try testing.expect(std.mem.indexOf(u8, writer.buffered(), "reason=\"AccessDenied\"") != null);
}

pub fn appendPromMetricLabeled(
    writer: *std.Io.Writer,
    name: []const u8,
    metric_type: []const u8,
    help: []const u8,
    labels: []const PromLabel,
    value: u64,
) !void {
    try appendPromMetricHeader(writer, name, metric_type, help);
    try appendPromSampleLabeled(writer, name, labels, value);
}

pub fn appendPromMetricHeader(
    writer: *std.Io.Writer,
    name: []const u8,
    metric_type: []const u8,
    help: []const u8,
) !void {
    try writer.print("# HELP {s} {s}\n# TYPE {s} {s}\n", .{ name, help, name, metric_type });
}

pub fn appendPromSample(writer: *std.Io.Writer, name: []const u8, value: u64) !void {
    try writer.print("{s} {d}\n", .{ name, value });
}

pub fn appendPromSampleLabeled(
    writer: *std.Io.Writer,
    name: []const u8,
    labels: []const PromLabel,
    value: u64,
) !void {
    try writer.print("{s}", .{name});
    try appendPromLabels(writer, labels);
    try writer.print(" {d}\n", .{value});
}

fn appendPromLabels(writer: *std.Io.Writer, labels: []const PromLabel) !void {
    if (labels.len == 0) return;
    try writer.print("{{", .{});
    for (labels, 0..) |label, i| {
        if (i > 0) try writer.print(",", .{});
        try writer.print("{s}=\"", .{label.name});
        try appendPromLabelValue(writer, label.value);
        try writer.print("\"", .{});
    }
    try writer.print("}}", .{});
}

fn appendPromLabelValue(writer: *std.Io.Writer, value: []const u8) !void {
    for (value) |c| {
        switch (c) {
            '\\' => try writer.print("\\\\", .{}),
            '"' => try writer.print("\\\"", .{}),
            '\n' => try writer.print("\\n", .{}),
            else => try writer.print("{c}", .{c}),
        }
    }
}

test "prometheus appendPromMetric formats correctly" {
    var buf: [256]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    try appendPromMetric(&writer, "my_metric", "gauge", "Help text", 7);
    const expected =
        "# HELP my_metric Help text\n" ++
        "# TYPE my_metric gauge\n" ++
        "my_metric 7\n";
    try testing.expectEqualStrings(expected, writer.buffered());
}

test "prometheus appendPromMetricLabeled formats and escapes labels" {
    var buf: [512]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    try appendPromMetricLabeled(
        &writer,
        "my_metric_total",
        "counter",
        "Help text",
        &.{
            .{ .name = "kind", .value = "run_table_index" },
            .{ .name = "path", .value = "quote\"slash\\line\n" },
        },
        9,
    );
    const expected =
        "# HELP my_metric_total Help text\n" ++
        "# TYPE my_metric_total counter\n" ++
        "my_metric_total{kind=\"run_table_index\",path=\"quote\\\"slash\\\\line\\n\"} 9\n";
    try testing.expectEqualStrings(expected, writer.buffered());
}
