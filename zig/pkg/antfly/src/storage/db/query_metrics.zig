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
const platform_sync = @import("antfly_platform").sync;
const types = @import("types.zig");

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
    sort_plan: []u8,
    sort_exactness: []u8,
    sort_source: []u8,
    sort_selection_reason: []u8,
    sort_rejection_reason: []u8,
    budget_rejection_reason: []u8,
    buckets: [bucket_bounds.len + 1]u64 = [_]u64{0} ** (bucket_bounds.len + 1),
    sum: f64 = 0,
    count: u64 = 0,
};

pub const SortMetricLabels = struct {
    plan: []const u8 = "",
    exactness: []const u8 = "",
    source: []const u8 = "",
    selection_reason: []const u8 = "",
    sort_rejection_reason: []const u8 = "",
    budget_rejection_reason: []const u8 = "",
};

pub const Collector = struct {
    alloc: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    pub fn init(alloc: std.mem.Allocator) Collector {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Collector) void {
        for (self.entries.items) |item| {
            self.alloc.free(item.name);
            self.alloc.free(item.sort_plan);
            self.alloc.free(item.sort_exactness);
            self.alloc.free(item.sort_source);
            self.alloc.free(item.sort_selection_reason);
            self.alloc.free(item.sort_rejection_reason);
            self.alloc.free(item.budget_rejection_reason);
        }
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn observe(self: *Collector, name: []const u8, query_type: QueryType, duration_ns: u64) !void {
        try self.observeWithSortLabels(name, query_type, duration_ns, .{});
    }

    pub fn observeWithSortLabels(
        self: *Collector,
        name: []const u8,
        query_type: QueryType,
        duration_ns: u64,
        sort: SortMetricLabels,
    ) !void {
        const item = try self.getOrCreateEntry(name, query_type, sort);
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
                try writeHistogramBucket(writer, item, label, item.buckets[i]);
            }
            try writeHistogramBucket(writer, item, "+Inf", item.buckets[bucket_bounds.len]);
            try writeHistogramSample(writer, "_sum", item, item.sum);
            try writeHistogramSample(writer, "_count", item, item.count);
        }
    }

    fn getOrCreateEntry(self: *Collector, name: []const u8, query_type: QueryType, sort: SortMetricLabels) !*Entry {
        for (self.entries.items) |*existing| {
            if (existing.query_type == query_type and
                std.mem.eql(u8, existing.name, name) and
                std.mem.eql(u8, existing.sort_plan, sort.plan) and
                std.mem.eql(u8, existing.sort_exactness, sort.exactness) and
                std.mem.eql(u8, existing.sort_source, sort.source) and
                std.mem.eql(u8, existing.sort_selection_reason, sort.selection_reason) and
                std.mem.eql(u8, existing.sort_rejection_reason, sort.sort_rejection_reason) and
                std.mem.eql(u8, existing.budget_rejection_reason, sort.budget_rejection_reason))
            {
                return existing;
            }
        }

        const owned_name = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(owned_name);
        const owned_sort_plan = try self.alloc.dupe(u8, sort.plan);
        errdefer self.alloc.free(owned_sort_plan);
        const owned_sort_exactness = try self.alloc.dupe(u8, sort.exactness);
        errdefer self.alloc.free(owned_sort_exactness);
        const owned_sort_source = try self.alloc.dupe(u8, sort.source);
        errdefer self.alloc.free(owned_sort_source);
        const owned_sort_selection_reason = try self.alloc.dupe(u8, sort.selection_reason);
        errdefer self.alloc.free(owned_sort_selection_reason);
        const owned_sort_rejection_reason = try self.alloc.dupe(u8, sort.sort_rejection_reason);
        errdefer self.alloc.free(owned_sort_rejection_reason);
        const owned_budget_rejection_reason = try self.alloc.dupe(u8, sort.budget_rejection_reason);
        errdefer self.alloc.free(owned_budget_rejection_reason);
        try self.entries.append(self.alloc, .{
            .name = owned_name,
            .query_type = query_type,
            .sort_plan = owned_sort_plan,
            .sort_exactness = owned_sort_exactness,
            .sort_source = owned_sort_source,
            .sort_selection_reason = owned_sort_selection_reason,
            .sort_rejection_reason = owned_sort_rejection_reason,
            .budget_rejection_reason = owned_budget_rejection_reason,
        });
        return &self.entries.items[self.entries.items.len - 1];
    }
};

const MetricsMutex = struct {
    state: std.atomic.Mutex = .unlocked,

    fn lock(self: *@This()) void {
        platform_sync.lockYielding(&self.state);
    }

    fn unlock(self: *@This()) void {
        self.state.unlock();
    }
};

var default_mutex: MetricsMutex = .{};
var default_collector: Collector = Collector.init(std.heap.page_allocator);

pub fn observe(name: ?[]const u8, query_type: QueryType, duration_ns: u64) void {
    observeWithSortLabels(name, query_type, duration_ns, .{});
}

pub fn observeSortProfile(name: ?[]const u8, query_type: QueryType, duration_ns: u64, sort_profile: ?types.SortProfile) void {
    const profile = sort_profile orelse return observe(name, query_type, duration_ns);
    observeWithSortLabels(name, query_type, duration_ns, .{
        .plan = profile.plan,
        .exactness = profile.exactness,
        .source = profile.source,
        .selection_reason = profile.selection_reason,
        .sort_rejection_reason = profile.sort_rejection_reason,
        .budget_rejection_reason = profile.budget_rejection_reason,
    });
}

pub fn observeSortRejection(name: ?[]const u8, query_type: QueryType, duration_ns: u64, reason: []const u8, detail: []const u8) void {
    observeWithSortLabels(name, query_type, duration_ns, sortRejectionMetricLabels(reason, detail));
}

pub fn observeWithSortLabels(name: ?[]const u8, query_type: QueryType, duration_ns: u64, sort: SortMetricLabels) void {
    const resolved_name = name orelse return;
    default_mutex.lock();
    defer default_mutex.unlock();
    default_collector.observeWithSortLabels(resolved_name, query_type, duration_ns, sort) catch |err| {
        std.log.err("failed to record index query latency metric: {s}", .{@errorName(err)});
    };
}

fn sortRejectionMetricLabels(reason: []const u8, detail: []const u8) SortMetricLabels {
    return .{
        .plan = "unsupported_exact_sort",
        .exactness = "unsupported",
        .source = "unsupported",
        .selection_reason = "unsupported_exact_sort",
        .sort_rejection_reason = reason,
        .budget_rejection_reason = if (std.mem.eql(u8, reason, "candidate_budget_exceeded")) detail else "",
    };
}

pub fn writePrometheus(writer: *std.Io.Writer) !void {
    default_mutex.lock();
    defer default_mutex.unlock();
    try default_collector.writePrometheus(writer);
}

fn writeHistogramBucket(
    writer: *std.Io.Writer,
    entry: Entry,
    le: []const u8,
    value: u64,
) !void {
    try writer.print("{s}_bucket{{Name=\"", .{metric_name});
    try writePromLabelValue(writer, entry.name);
    try writer.print("\",query_type=\"", .{});
    try writePromLabelValue(writer, entry.query_type.label());
    try writer.print("\",sort_plan=\"", .{});
    try writePromLabelValue(writer, entry.sort_plan);
    try writer.print("\",sort_exactness=\"", .{});
    try writePromLabelValue(writer, entry.sort_exactness);
    try writer.print("\",sort_source=\"", .{});
    try writePromLabelValue(writer, entry.sort_source);
    try writer.print("\",sort_selection_reason=\"", .{});
    try writePromLabelValue(writer, entry.sort_selection_reason);
    try writer.print("\",sort_rejection_reason=\"", .{});
    try writePromLabelValue(writer, entry.sort_rejection_reason);
    try writer.print("\",budget_rejection_reason=\"", .{});
    try writePromLabelValue(writer, entry.budget_rejection_reason);
    try writer.print("\",le=\"", .{});
    try writePromLabelValue(writer, le);
    try writer.print("\"}} {d}\n", .{value});
}

fn writeHistogramSample(
    writer: *std.Io.Writer,
    suffix: []const u8,
    entry: Entry,
    value: anytype,
) !void {
    try writer.print("{s}{s}{{Name=\"", .{ metric_name, suffix });
    try writePromLabelValue(writer, entry.name);
    try writer.print("\",query_type=\"", .{});
    try writePromLabelValue(writer, entry.query_type.label());
    try writer.print("\",sort_plan=\"", .{});
    try writePromLabelValue(writer, entry.sort_plan);
    try writer.print("\",sort_exactness=\"", .{});
    try writePromLabelValue(writer, entry.sort_exactness);
    try writer.print("\",sort_source=\"", .{});
    try writePromLabelValue(writer, entry.sort_source);
    try writer.print("\",sort_selection_reason=\"", .{});
    try writePromLabelValue(writer, entry.sort_selection_reason);
    try writer.print("\",sort_rejection_reason=\"", .{});
    try writePromLabelValue(writer, entry.sort_rejection_reason);
    try writer.print("\",budget_rejection_reason=\"", .{});
    try writePromLabelValue(writer, entry.budget_rejection_reason);
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
    try collector.observeWithSortLabels("docs", .search, 2 * std.time.ns_per_ms, .{
        .plan = "native_doc_values_top_n",
        .exactness = "exact",
        .source = "doc_values_collector",
        .selection_reason = "doc_values_collector",
        .sort_rejection_reason = "missing_doc_values_coverage",
        .budget_rejection_reason = "match_all_candidate_collect_limit",
    });
    try collector.observe("vec\"tors", .vector, 2 * std.time.ns_per_s);

    var writer_buf: [32768]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try collector.writePrometheus(&writer);
    const output = writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE antfly_indexes_query_duration_seconds histogram") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_bucket{Name=\"docs\",query_type=\"search\",sort_plan=\"\",sort_exactness=\"\",sort_source=\"\",sort_selection_reason=\"\",sort_rejection_reason=\"\",budget_rejection_reason=\"\",le=\"0.001\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_count{Name=\"docs\",query_type=\"search\",sort_plan=\"\",sort_exactness=\"\",sort_source=\"\",sort_selection_reason=\"\",sort_rejection_reason=\"\",budget_rejection_reason=\"\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_bucket{Name=\"docs\",query_type=\"search\",sort_plan=\"native_doc_values_top_n\",sort_exactness=\"exact\",sort_source=\"doc_values_collector\",sort_selection_reason=\"doc_values_collector\",sort_rejection_reason=\"missing_doc_values_coverage\",budget_rejection_reason=\"match_all_candidate_collect_limit\",le=\"0.005\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_bucket{Name=\"vec\\\"tors\",query_type=\"vector\",sort_plan=\"\",sort_exactness=\"\",sort_source=\"\",sort_selection_reason=\"\",sort_rejection_reason=\"\",budget_rejection_reason=\"\",le=\"2.5\"} 1") != null);
}

test "sort rejection metrics use stable unsupported exact-sort labels" {
    var collector = Collector.init(std.testing.allocator);
    defer collector.deinit();

    const labels = sortRejectionMetricLabels("candidate_budget_exceeded", "match_all_candidate_collect_limit");
    try std.testing.expectEqualStrings("unsupported_exact_sort", labels.plan);
    try std.testing.expectEqualStrings("unsupported", labels.exactness);
    try std.testing.expectEqualStrings("unsupported", labels.source);
    try std.testing.expectEqualStrings("unsupported_exact_sort", labels.selection_reason);
    try std.testing.expectEqualStrings("candidate_budget_exceeded", labels.sort_rejection_reason);
    try std.testing.expectEqualStrings("match_all_candidate_collect_limit", labels.budget_rejection_reason);

    const non_budget_labels = sortRejectionMetricLabels("missing_doc_values_coverage", "missing_doc_values_section");
    try std.testing.expectEqualStrings("missing_doc_values_coverage", non_budget_labels.sort_rejection_reason);
    try std.testing.expectEqualStrings("", non_budget_labels.budget_rejection_reason);

    try collector.observeWithSortLabels("docs", .search, 2 * std.time.ns_per_ms, labels);

    var writer_buf: [32768]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try collector.writePrometheus(&writer);
    const output = writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, output, "sort_plan=\"unsupported_exact_sort\",sort_exactness=\"unsupported\",sort_source=\"unsupported\",sort_selection_reason=\"unsupported_exact_sort\",sort_rejection_reason=\"candidate_budget_exceeded\",budget_rejection_reason=\"match_all_candidate_collect_limit\"") != null);
}
