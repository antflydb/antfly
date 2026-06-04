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

const metric_name = "antfly_indexes_query_duration_seconds";
const metric_help = "Index query latency in seconds.";
const bucket_bounds = [_]f64{ 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 };
const bucket_labels = [_][]const u8{ "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10" };

pub const QueryType = enum {
    search,
    vector,

    fn label(self: QueryType) []const u8 {
        return switch (self) {
            .search => "search",
            .vector => "vector",
        };
    }
};

const Entry = struct {
    name: []u8,
    query_type: QueryType,
    buckets: [bucket_bounds.len + 1]u64 = @as([(bucket_bounds.len + 1)]u64, @splat(0)),
    sum: f64 = 0,
    count: u64 = 0,
};

pub const Collector = struct {
    alloc: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    pub fn init(alloc: std.mem.Allocator) Collector {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Collector) void {
        for (self.entries.items) |item| self.alloc.free(item.name);
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn observe(self: *Collector, name: []const u8, query_type: QueryType, duration_ns: u64) !void {
        const item = try self.getOrCreateEntry(name, query_type);
        const seconds: f64 = @as(f64, @floatFromInt(duration_ns)) / @as(f64, @floatFromInt(std.time.ns_per_s));

        for (bucket_bounds, 0..) |upper, i| {
            if (seconds <= upper) item.buckets[i] +|= 1;
        }
        item.buckets[bucket_bounds.len] +|= 1;
        item.sum += seconds;
        item.count +|= 1;
    }

    pub fn writePrometheus(self: *const Collector, writer: *std.Io.Writer) !void {
        try writer.print("# HELP {s} {s}\n# TYPE {s} histogram\n", .{ metric_name, metric_help, metric_name });
        for (self.entries.items) |item| {
            for (bucket_labels, 0..) |label, i| {
                try writeHistogramBucket(writer, item.name, item.query_type.label(), label, item.buckets[i]);
            }
            try writeHistogramBucket(writer, item.name, item.query_type.label(), "+Inf", item.buckets[bucket_bounds.len]);
            try writeHistogramSample(writer, "_sum", item.name, item.query_type.label(), item.sum);
            try writeHistogramSample(writer, "_count", item.name, item.query_type.label(), item.count);
        }
    }

    fn getOrCreateEntry(self: *Collector, name: []const u8, query_type: QueryType) !*Entry {
        for (self.entries.items) |*existing| {
            if (existing.query_type == query_type and std.mem.eql(u8, existing.name, name)) return existing;
        }

        const owned_name = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(owned_name);
        try self.entries.append(self.alloc, .{
            .name = owned_name,
            .query_type = query_type,
        });
        return &self.entries.items[self.entries.items.len - 1];
    }
};

const MetricsMutex = struct {
    state: std.atomic.Mutex = .unlocked,

    fn lock(self: *@This()) void {
        while (!self.state.tryLock()) std.atomic.spinLoopHint();
    }

    fn unlock(self: *@This()) void {
        self.state.unlock();
    }
};

var default_mutex: MetricsMutex = .{};
var default_collector: Collector = Collector.init(std.heap.page_allocator);

pub fn observe(name: ?[]const u8, query_type: QueryType, duration_ns: u64) void {
    const resolved_name = name orelse return;
    default_mutex.lock();
    defer default_mutex.unlock();
    default_collector.observe(resolved_name, query_type, duration_ns) catch |err| {
        std.log.err("failed to record index query latency metric: {s}", .{@errorName(err)});
    };
}

pub fn writePrometheus(writer: *std.Io.Writer) !void {
    default_mutex.lock();
    defer default_mutex.unlock();
    try default_collector.writePrometheus(writer);
}

fn writeHistogramBucket(
    writer: *std.Io.Writer,
    name: []const u8,
    query_type: []const u8,
    le: []const u8,
    value: u64,
) !void {
    try writer.print("{s}_bucket{{Name=\"", .{metric_name});
    try writePromLabelValue(writer, name);
    try writer.print("\",query_type=\"", .{});
    try writePromLabelValue(writer, query_type);
    try writer.print("\",le=\"", .{});
    try writePromLabelValue(writer, le);
    try writer.print("\"}} {d}\n", .{value});
}

fn writeHistogramSample(
    writer: *std.Io.Writer,
    suffix: []const u8,
    name: []const u8,
    query_type: []const u8,
    value: anytype,
) !void {
    try writer.print("{s}{s}{{Name=\"", .{ metric_name, suffix });
    try writePromLabelValue(writer, name);
    try writer.print("\",query_type=\"", .{});
    try writePromLabelValue(writer, query_type);
    try writer.print("\"}} {d}\n", .{value});
}

fn writePromLabelValue(writer: *std.Io.Writer, value: []const u8) !void {
    for (value) |c| {
        switch (c) {
            '\\' => try writer.print("\\\\", .{}),
            '"' => try writer.print("\\\"", .{}),
            '\n' => try writer.print("\\n", .{}),
            else => try writer.print("{c}", .{c}),
        }
    }
}

test "collector writes Prometheus histogram for index query latency" {
    var collector = Collector.init(std.testing.allocator);
    defer collector.deinit();

    try collector.observe("docs", .search, std.time.ns_per_ms);
    try collector.observe("vec\"tors", .vector, 2 * std.time.ns_per_s);

    var writer_buf: [8192]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try collector.writePrometheus(&writer);
    const output = writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE antfly_indexes_query_duration_seconds histogram") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_bucket{Name=\"docs\",query_type=\"search\",le=\"0.001\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_bucket{Name=\"docs\",query_type=\"search\",le=\"+Inf\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_count{Name=\"docs\",query_type=\"search\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_bucket{Name=\"vec\\\"tors\",query_type=\"vector\",le=\"2.5\"} 1") != null);
}
