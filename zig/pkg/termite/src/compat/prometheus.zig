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

const MetricKind = enum {
    counter,
    gauge,
};

fn MetricState(comptime T: type, comptime kind: MetricKind) type {
    _ = kind;
    return struct {
        name: []const u8,
        help: []const u8,
        value: T,

        const Self = @This();

        pub fn init(name: []const u8, desc: anytype, _: anytype) Self {
            return .{
                .name = name,
                .help = if (@hasField(@TypeOf(desc), "help")) desc.help else "",
                .value = 0,
            };
        }

        pub fn incr(self: *Self) void {
            self.value += 1;
        }

        pub fn incrBy(self: *Self, amount: T) void {
            self.value += amount;
        }

        pub fn set(self: *Self, value: T) void {
            self.value = value;
        }
    };
}

pub fn Counter(comptime T: type) type {
    return MetricState(T, .counter);
}

pub fn Gauge(comptime T: type) type {
    return MetricState(T, .gauge);
}

fn writeMetric(writer: *std.Io.Writer, metric: anytype) !void {
    const metric_type = @TypeOf(metric.*);
    const info = @typeInfo(metric_type);
    const kind = if (std.mem.indexOf(u8, @typeName(metric_type), "Counter(") != null)
        "counter"
    else
        "gauge";

    if (info != .@"struct") return;
    if (!@hasField(metric_type, "name") or !@hasField(metric_type, "value")) return;

    if (@hasField(metric_type, "help") and metric.help.len != 0) {
        try writer.print("# HELP {s} {s}\n", .{ metric.name, metric.help });
    }
    try writer.print("# TYPE {s} {s}\n", .{ metric.name, kind });
    try writer.print("{s} {}\n", .{ metric.name, metric.value });
}

pub fn write(metrics: anytype, writer: *std.Io.Writer) !void {
    inline for (std.meta.fields(@TypeOf(metrics.*))) |field| {
        try writeMetric(writer, &@field(metrics, field.name));
    }
}

test "counter and gauge render" {
    var counter = Counter(u64).init("requests_total", .{ .help = "Total requests" }, .{});
    counter.incr();
    counter.incrBy(2);

    var gauge = Gauge(i64).init("requests_active", .{ .help = "Active requests" }, .{});
    gauge.set(3);
    gauge.incrBy(-1);

    const Sample = struct {
        counter: Counter(u64),
        gauge: Gauge(i64),
    };

    var sample = Sample{
        .counter = counter,
        .gauge = gauge,
    };

    var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer out.deinit();
    try write(&sample, &out.writer);

    const rendered = out.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, rendered, "requests_total 3\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "requests_active 2\n") != null);
}
