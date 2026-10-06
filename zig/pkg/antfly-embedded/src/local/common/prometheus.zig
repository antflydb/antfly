// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
