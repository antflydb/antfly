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
const sort_plan_metric_name = "antfly_indexes_sort_plan_total";
const sort_plan_metric_help = "Sort plan selections by bounded planner labels.";
const sort_failure_metric_name = "antfly_indexes_sort_failures_total";
const sort_failure_metric_help = "Sort planning or execution failures by stable reason.";
const sort_executor_metric_name = "antfly_indexes_sort_executor_duration_seconds";
const sort_executor_metric_help = "Sort executor latency in seconds.";
const sort_candidate_metric_name = "antfly_indexes_sort_candidate_count";
const sort_candidate_metric_help = "Native sort candidate count.";
const sort_selected_metric_name = "antfly_indexes_sort_selected_count";
const sort_selected_metric_help = "Sort selected hit count.";
const bucket_bounds = [_]f64{ 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 };
const bucket_labels = [_][]const u8{ "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10" };
const count_bucket_bounds = [_]u64{ 0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000 };
const count_bucket_labels = [_][]const u8{ "0", "1", "2", "5", "10", "25", "50", "100", "250", "500", "1000", "5000", "10000" };

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
    sort_candidate_source: []u8,
    sort_selection_reason: []u8,
    sort_rejection_reason: []u8,
    budget_rejection_reason: []u8,
    sort_cursor_mode: []u8,
    buckets: [bucket_bounds.len + 1]u64 = [_]u64{0} ** (bucket_bounds.len + 1),
    sum: f64 = 0,
    count: u64 = 0,
};

const SortEntry = struct {
    query_type: QueryType,
    plan: []u8,
    exactness: []u8,
    source: []u8,
    cursor_mode: []u8,
    total: u64 = 0,
    executor_buckets: [bucket_bounds.len + 1]u64 = [_]u64{0} ** (bucket_bounds.len + 1),
    executor_sum: f64 = 0,
    executor_count: u64 = 0,
    candidate_buckets: [count_bucket_bounds.len + 1]u64 = [_]u64{0} ** (count_bucket_bounds.len + 1),
    candidate_sum: u64 = 0,
    candidate_count: u64 = 0,
    selected_buckets: [count_bucket_bounds.len + 1]u64 = [_]u64{0} ** (count_bucket_bounds.len + 1),
    selected_sum: u64 = 0,
    selected_count: u64 = 0,
};

const SortFailureEntry = struct {
    query_type: QueryType,
    reason: []u8,
    total: u64 = 0,
};

pub const SortMetricLabels = struct {
    plan: []const u8 = "",
    exactness: []const u8 = "",
    source: []const u8 = "",
    candidate_source: []const u8 = "",
    selection_reason: []const u8 = "",
    sort_rejection_reason: []const u8 = "",
    budget_rejection_reason: []const u8 = "",
    cursor_mode: []const u8 = "",
};

pub const Collector = struct {
    alloc: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    sort_entries: std.ArrayListUnmanaged(SortEntry) = .empty,
    sort_failure_entries: std.ArrayListUnmanaged(SortFailureEntry) = .empty,

    pub fn init(alloc: std.mem.Allocator) Collector {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Collector) void {
        for (self.entries.items) |item| {
            self.alloc.free(item.name);
            self.alloc.free(item.sort_plan);
            self.alloc.free(item.sort_exactness);
            self.alloc.free(item.sort_source);
            self.alloc.free(item.sort_candidate_source);
            self.alloc.free(item.sort_selection_reason);
            self.alloc.free(item.sort_rejection_reason);
            self.alloc.free(item.budget_rejection_reason);
            self.alloc.free(item.sort_cursor_mode);
        }
        self.entries.deinit(self.alloc);
        for (self.sort_entries.items) |item| {
            self.alloc.free(item.plan);
            self.alloc.free(item.exactness);
            self.alloc.free(item.source);
            self.alloc.free(item.cursor_mode);
        }
        self.sort_entries.deinit(self.alloc);
        for (self.sort_failure_entries.items) |item| {
            self.alloc.free(item.reason);
        }
        self.sort_failure_entries.deinit(self.alloc);
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

    pub fn observeSortProfile(self: *Collector, query_type: QueryType, profile: types.SortProfile) !void {
        const item = try self.getOrCreateSortEntry(query_type, profile);
        item.total +|= 1;
        observeDurationHistogram(&item.executor_buckets, &item.executor_sum, &item.executor_count, usToSeconds(profile.total_us));
        observeCountHistogram(&item.candidate_buckets, &item.candidate_sum, &item.candidate_count, profile.candidate_count);
        observeCountHistogram(&item.selected_buckets, &item.selected_sum, &item.selected_count, profile.selected_count);

        if (profile.sort_rejection_reason.len > 0) {
            try self.observeSortFailure(query_type, profile.sort_rejection_reason);
        } else if (profile.budget_rejection_reason.len > 0) {
            try self.observeSortFailure(query_type, "candidate_budget_exceeded");
        }
    }

    pub fn observeSortFailure(self: *Collector, query_type: QueryType, reason: []const u8) !void {
        const item = try self.getOrCreateSortFailureEntry(query_type, reason);
        item.total +|= 1;
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
        try writer.print("# HELP {s} {s}\n# TYPE {s} counter\n", .{ sort_plan_metric_name, sort_plan_metric_help, sort_plan_metric_name });
        for (self.sort_entries.items) |item| {
            try writeSortPlanCounter(writer, item);
        }
        try writer.print("# HELP {s} {s}\n# TYPE {s} counter\n", .{ sort_failure_metric_name, sort_failure_metric_help, sort_failure_metric_name });
        for (self.sort_failure_entries.items) |item| {
            try writeSortFailureCounter(writer, item);
        }
        try writer.print("# HELP {s} {s}\n# TYPE {s} histogram\n", .{ sort_executor_metric_name, sort_executor_metric_help, sort_executor_metric_name });
        for (self.sort_entries.items) |item| {
            for (bucket_labels, 0..) |label, i| {
                try writeSortDurationHistogramBucket(writer, sort_executor_metric_name, item, label, item.executor_buckets[i]);
            }
            try writeSortDurationHistogramBucket(writer, sort_executor_metric_name, item, "+Inf", item.executor_buckets[bucket_bounds.len]);
            try writeSortDurationHistogramSample(writer, sort_executor_metric_name, "_sum", item, item.executor_sum);
            try writeSortDurationHistogramSample(writer, sort_executor_metric_name, "_count", item, item.executor_count);
        }
        try writer.print("# HELP {s} {s}\n# TYPE {s} histogram\n", .{ sort_candidate_metric_name, sort_candidate_metric_help, sort_candidate_metric_name });
        for (self.sort_entries.items) |item| {
            for (count_bucket_labels, 0..) |label, i| {
                try writeSortCountHistogramBucket(writer, sort_candidate_metric_name, item, label, item.candidate_buckets[i]);
            }
            try writeSortCountHistogramBucket(writer, sort_candidate_metric_name, item, "+Inf", item.candidate_buckets[count_bucket_bounds.len]);
            try writeSortCountHistogramSample(writer, sort_candidate_metric_name, "_sum", item, item.candidate_sum);
            try writeSortCountHistogramSample(writer, sort_candidate_metric_name, "_count", item, item.candidate_count);
        }
        try writer.print("# HELP {s} {s}\n# TYPE {s} histogram\n", .{ sort_selected_metric_name, sort_selected_metric_help, sort_selected_metric_name });
        for (self.sort_entries.items) |item| {
            for (count_bucket_labels, 0..) |label, i| {
                try writeSortCountHistogramBucket(writer, sort_selected_metric_name, item, label, item.selected_buckets[i]);
            }
            try writeSortCountHistogramBucket(writer, sort_selected_metric_name, item, "+Inf", item.selected_buckets[count_bucket_bounds.len]);
            try writeSortCountHistogramSample(writer, sort_selected_metric_name, "_sum", item, item.selected_sum);
            try writeSortCountHistogramSample(writer, sort_selected_metric_name, "_count", item, item.selected_count);
        }
    }

    fn getOrCreateEntry(self: *Collector, name: []const u8, query_type: QueryType, sort: SortMetricLabels) !*Entry {
        for (self.entries.items) |*existing| {
            if (existing.query_type == query_type and
                std.mem.eql(u8, existing.name, name) and
                std.mem.eql(u8, existing.sort_plan, sort.plan) and
                std.mem.eql(u8, existing.sort_exactness, sort.exactness) and
                std.mem.eql(u8, existing.sort_source, sort.source) and
                std.mem.eql(u8, existing.sort_candidate_source, sort.candidate_source) and
                std.mem.eql(u8, existing.sort_selection_reason, sort.selection_reason) and
                std.mem.eql(u8, existing.sort_rejection_reason, sort.sort_rejection_reason) and
                std.mem.eql(u8, existing.budget_rejection_reason, sort.budget_rejection_reason) and
                std.mem.eql(u8, existing.sort_cursor_mode, sort.cursor_mode))
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
        const owned_sort_candidate_source = try self.alloc.dupe(u8, sort.candidate_source);
        errdefer self.alloc.free(owned_sort_candidate_source);
        const owned_sort_selection_reason = try self.alloc.dupe(u8, sort.selection_reason);
        errdefer self.alloc.free(owned_sort_selection_reason);
        const owned_sort_rejection_reason = try self.alloc.dupe(u8, sort.sort_rejection_reason);
        errdefer self.alloc.free(owned_sort_rejection_reason);
        const owned_budget_rejection_reason = try self.alloc.dupe(u8, sort.budget_rejection_reason);
        errdefer self.alloc.free(owned_budget_rejection_reason);
        const owned_sort_cursor_mode = try self.alloc.dupe(u8, sort.cursor_mode);
        errdefer self.alloc.free(owned_sort_cursor_mode);
        try self.entries.append(self.alloc, .{
            .name = owned_name,
            .query_type = query_type,
            .sort_plan = owned_sort_plan,
            .sort_exactness = owned_sort_exactness,
            .sort_source = owned_sort_source,
            .sort_candidate_source = owned_sort_candidate_source,
            .sort_selection_reason = owned_sort_selection_reason,
            .sort_rejection_reason = owned_sort_rejection_reason,
            .budget_rejection_reason = owned_budget_rejection_reason,
            .sort_cursor_mode = owned_sort_cursor_mode,
        });
        return &self.entries.items[self.entries.items.len - 1];
    }

    fn getOrCreateSortEntry(self: *Collector, query_type: QueryType, profile: types.SortProfile) !*SortEntry {
        const labels = sortMetricLabelsFromProfile(profile);
        for (self.sort_entries.items) |*existing| {
            if (existing.query_type == query_type and
                std.mem.eql(u8, existing.plan, labels.plan) and
                std.mem.eql(u8, existing.exactness, labels.exactness) and
                std.mem.eql(u8, existing.source, labels.source) and
                std.mem.eql(u8, existing.cursor_mode, labels.cursor_mode))
            {
                return existing;
            }
        }

        const owned_plan = try self.alloc.dupe(u8, labels.plan);
        errdefer self.alloc.free(owned_plan);
        const owned_exactness = try self.alloc.dupe(u8, labels.exactness);
        errdefer self.alloc.free(owned_exactness);
        const owned_source = try self.alloc.dupe(u8, labels.source);
        errdefer self.alloc.free(owned_source);
        const owned_cursor_mode = try self.alloc.dupe(u8, labels.cursor_mode);
        errdefer self.alloc.free(owned_cursor_mode);
        try self.sort_entries.append(self.alloc, .{
            .query_type = query_type,
            .plan = owned_plan,
            .exactness = owned_exactness,
            .source = owned_source,
            .cursor_mode = owned_cursor_mode,
        });
        return &self.sort_entries.items[self.sort_entries.items.len - 1];
    }

    fn getOrCreateSortFailureEntry(self: *Collector, query_type: QueryType, reason: []const u8) !*SortFailureEntry {
        for (self.sort_failure_entries.items) |*existing| {
            if (existing.query_type == query_type and std.mem.eql(u8, existing.reason, reason)) return existing;
        }

        const owned_reason = try self.alloc.dupe(u8, reason);
        errdefer self.alloc.free(owned_reason);
        try self.sort_failure_entries.append(self.alloc, .{
            .query_type = query_type,
            .reason = owned_reason,
        });
        return &self.sort_failure_entries.items[self.sort_failure_entries.items.len - 1];
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
    const labels = sortMetricLabelsFromProfile(profile);
    default_mutex.lock();
    defer default_mutex.unlock();
    if (name) |resolved_name| {
        default_collector.observeWithSortLabels(resolved_name, query_type, duration_ns, labels) catch |err| {
            std.log.err("failed to record index query latency metric: {s}", .{@errorName(err)});
        };
    }
    default_collector.observeSortProfile(query_type, profile) catch |err| {
        std.log.err("failed to record index sort profile metric: {s}", .{@errorName(err)});
    };
}

pub fn observeSortRejection(name: ?[]const u8, query_type: QueryType, duration_ns: u64, reason: []const u8, detail: []const u8) void {
    const labels = sortRejectionMetricLabels(reason, detail);
    default_mutex.lock();
    defer default_mutex.unlock();
    if (name) |resolved_name| {
        default_collector.observeWithSortLabels(resolved_name, query_type, duration_ns, labels) catch |err| {
            std.log.err("failed to record index query latency metric: {s}", .{@errorName(err)});
        };
    }
    default_collector.observeSortFailure(query_type, reason) catch |err| {
        std.log.err("failed to record index sort failure metric: {s}", .{@errorName(err)});
    };
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
        .cursor_mode = "none",
    };
}

fn sortMetricLabelsFromProfile(profile: types.SortProfile) SortMetricLabels {
    return .{
        .plan = profile.plan,
        .exactness = profile.exactness,
        .source = profile.source,
        .candidate_source = profile.candidate_source,
        .selection_reason = profile.selection_reason,
        .sort_rejection_reason = profile.sort_rejection_reason,
        .budget_rejection_reason = profile.budget_rejection_reason,
        .cursor_mode = if (profile.cursor_support.len > 0) profile.cursor_support else "none",
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
    try writer.print("\",sort_candidate_source=\"", .{});
    try writePromLabelValue(writer, entry.sort_candidate_source);
    try writer.print("\",sort_selection_reason=\"", .{});
    try writePromLabelValue(writer, entry.sort_selection_reason);
    try writer.print("\",sort_rejection_reason=\"", .{});
    try writePromLabelValue(writer, entry.sort_rejection_reason);
    try writer.print("\",budget_rejection_reason=\"", .{});
    try writePromLabelValue(writer, entry.budget_rejection_reason);
    try writer.print("\",sort_cursor_mode=\"", .{});
    try writePromLabelValue(writer, entry.sort_cursor_mode);
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
    try writer.print("\",sort_candidate_source=\"", .{});
    try writePromLabelValue(writer, entry.sort_candidate_source);
    try writer.print("\",sort_selection_reason=\"", .{});
    try writePromLabelValue(writer, entry.sort_selection_reason);
    try writer.print("\",sort_rejection_reason=\"", .{});
    try writePromLabelValue(writer, entry.sort_rejection_reason);
    try writer.print("\",budget_rejection_reason=\"", .{});
    try writePromLabelValue(writer, entry.budget_rejection_reason);
    try writer.print("\",sort_cursor_mode=\"", .{});
    try writePromLabelValue(writer, entry.sort_cursor_mode);
    try writer.print("\"}} {d}\n", .{value});
}

fn observeDurationHistogram(
    buckets: *[bucket_bounds.len + 1]u64,
    sum: *f64,
    count: *u64,
    seconds: f64,
) void {
    for (bucket_bounds, 0..) |upper, i| {
        if (seconds <= upper) buckets[i] +|= 1;
    }
    buckets[bucket_bounds.len] +|= 1;
    sum.* += seconds;
    count.* +|= 1;
}

fn observeCountHistogram(
    buckets: *[count_bucket_bounds.len + 1]u64,
    sum: *u64,
    count: *u64,
    value: u64,
) void {
    for (count_bucket_bounds, 0..) |upper, i| {
        if (value <= upper) buckets[i] +|= 1;
    }
    buckets[count_bucket_bounds.len] +|= 1;
    sum.* +|= value;
    count.* +|= 1;
}

fn usToSeconds(us: u64) f64 {
    return @as(f64, @floatFromInt(us)) / 1_000_000.0;
}

fn writeSortPlanCounter(writer: *std.Io.Writer, entry: SortEntry) !void {
    try writer.print("{s}{{query_type=\"", .{sort_plan_metric_name});
    try writePromLabelValue(writer, entry.query_type.label());
    try writer.print("\",plan=\"", .{});
    try writePromLabelValue(writer, entry.plan);
    try writer.print("\",exactness=\"", .{});
    try writePromLabelValue(writer, entry.exactness);
    try writer.print("\",source=\"", .{});
    try writePromLabelValue(writer, entry.source);
    try writer.print("\",cursor_mode=\"", .{});
    try writePromLabelValue(writer, entry.cursor_mode);
    try writer.print("\"}} {d}\n", .{entry.total});
}

fn writeSortFailureCounter(writer: *std.Io.Writer, entry: SortFailureEntry) !void {
    try writer.print("{s}{{query_type=\"", .{sort_failure_metric_name});
    try writePromLabelValue(writer, entry.query_type.label());
    try writer.print("\",reason=\"", .{});
    try writePromLabelValue(writer, entry.reason);
    try writer.print("\"}} {d}\n", .{entry.total});
}

fn writeSortLabels(writer: *std.Io.Writer, entry: SortEntry) !void {
    try writer.print("query_type=\"", .{});
    try writePromLabelValue(writer, entry.query_type.label());
    try writer.print("\",plan=\"", .{});
    try writePromLabelValue(writer, entry.plan);
    try writer.print("\",exactness=\"", .{});
    try writePromLabelValue(writer, entry.exactness);
    try writer.print("\",source=\"", .{});
    try writePromLabelValue(writer, entry.source);
    try writer.print("\",cursor_mode=\"", .{});
    try writePromLabelValue(writer, entry.cursor_mode);
    try writer.print("\"", .{});
}

fn writeSortDurationHistogramBucket(
    writer: *std.Io.Writer,
    name: []const u8,
    entry: SortEntry,
    le: []const u8,
    value: u64,
) !void {
    try writer.print("{s}_bucket{{", .{name});
    try writeSortLabels(writer, entry);
    try writer.print(",le=\"", .{});
    try writePromLabelValue(writer, le);
    try writer.print("\"}} {d}\n", .{value});
}

fn writeSortDurationHistogramSample(
    writer: *std.Io.Writer,
    name: []const u8,
    suffix: []const u8,
    entry: SortEntry,
    value: anytype,
) !void {
    try writer.print("{s}{s}{{", .{ name, suffix });
    try writeSortLabels(writer, entry);
    try writer.print("}} {d}\n", .{value});
}

fn writeSortCountHistogramBucket(
    writer: *std.Io.Writer,
    name: []const u8,
    entry: SortEntry,
    le: []const u8,
    value: u64,
) !void {
    try writer.print("{s}_bucket{{", .{name});
    try writeSortLabels(writer, entry);
    try writer.print(",le=\"", .{});
    try writePromLabelValue(writer, le);
    try writer.print("\"}} {d}\n", .{value});
}

fn writeSortCountHistogramSample(
    writer: *std.Io.Writer,
    name: []const u8,
    suffix: []const u8,
    entry: SortEntry,
    value: anytype,
) !void {
    try writer.print("{s}{s}{{", .{ name, suffix });
    try writeSortLabels(writer, entry);
    try writer.print("}} {d}\n", .{value});
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
        .candidate_source = "native_filter",
        .selection_reason = "doc_values_collector",
        .sort_rejection_reason = "missing_doc_values_coverage",
        .budget_rejection_reason = "match_all_candidate_collect_limit",
        .cursor_mode = "comparator",
    });
    try collector.observe("vec\"tors", .vector, 2 * std.time.ns_per_s);

    var writer_buf: [32768]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try collector.writePrometheus(&writer);
    const output = writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE antfly_indexes_query_duration_seconds histogram") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_bucket{Name=\"docs\",query_type=\"search\",sort_plan=\"\",sort_exactness=\"\",sort_source=\"\",sort_candidate_source=\"\",sort_selection_reason=\"\",sort_rejection_reason=\"\",budget_rejection_reason=\"\",sort_cursor_mode=\"\",le=\"0.001\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_count{Name=\"docs\",query_type=\"search\",sort_plan=\"\",sort_exactness=\"\",sort_source=\"\",sort_candidate_source=\"\",sort_selection_reason=\"\",sort_rejection_reason=\"\",budget_rejection_reason=\"\",sort_cursor_mode=\"\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_bucket{Name=\"docs\",query_type=\"search\",sort_plan=\"native_doc_values_top_n\",sort_exactness=\"exact\",sort_source=\"doc_values_collector\",sort_candidate_source=\"native_filter\",sort_selection_reason=\"doc_values_collector\",sort_rejection_reason=\"missing_doc_values_coverage\",budget_rejection_reason=\"match_all_candidate_collect_limit\",sort_cursor_mode=\"comparator\",le=\"0.005\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_query_duration_seconds_bucket{Name=\"vec\\\"tors\",query_type=\"vector\",sort_plan=\"\",sort_exactness=\"\",sort_source=\"\",sort_candidate_source=\"\",sort_selection_reason=\"\",sort_rejection_reason=\"\",budget_rejection_reason=\"\",sort_cursor_mode=\"\",le=\"2.5\"} 1") != null);
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

    try std.testing.expect(std.mem.indexOf(u8, output, "sort_plan=\"unsupported_exact_sort\",sort_exactness=\"unsupported\",sort_source=\"unsupported\",sort_candidate_source=\"\",sort_selection_reason=\"unsupported_exact_sort\",sort_rejection_reason=\"candidate_budget_exceeded\",budget_rejection_reason=\"match_all_candidate_collect_limit\",sort_cursor_mode=\"none\"") != null);
}

test "sort profile metrics use concise planner labels" {
    var collector = Collector.init(std.testing.allocator);
    defer collector.deinit();

    const profile = types.SortProfile{
        .plan = "sorted_segment_seek",
        .exactness = "exact",
        .source = "sorted_segment_scan",
        .candidate_source = "sorted_segment_membership",
        .cursor_support = "segment_seek",
        .selection_reason = "index_sort_sorted_segment_seek",
        .candidate_count = 13,
        .selected_count = 5,
        .total_us = 2000,
        .budget_rejection_reason = "",
        .sort_rejection_reason = "",
    };
    try collector.observeWithSortLabels("docs", .search, 3 * std.time.ns_per_ms, sortMetricLabelsFromProfile(profile));
    try collector.observeSortProfile(.search, profile);

    var writer_buf: [32768]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try collector.writePrometheus(&writer);
    const output = writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, output, "sort_plan=\"sorted_segment_seek\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "sort_exactness=\"exact\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "sort_source=\"sorted_segment_scan\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "sort_candidate_source=\"sorted_segment_membership\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "sort_selection_reason=\"index_sort_sorted_segment_seek\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "sort_rejection_reason=\"\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "budget_rejection_reason=\"\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "sort_cursor_mode=\"segment_seek\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_sort_plan_total{query_type=\"search\",plan=\"sorted_segment_seek\",exactness=\"exact\",source=\"sorted_segment_scan\",cursor_mode=\"segment_seek\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_sort_executor_duration_seconds_bucket{query_type=\"search\",plan=\"sorted_segment_seek\",exactness=\"exact\",source=\"sorted_segment_scan\",cursor_mode=\"segment_seek\",le=\"0.005\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_sort_candidate_count_bucket{query_type=\"search\",plan=\"sorted_segment_seek\",exactness=\"exact\",source=\"sorted_segment_scan\",cursor_mode=\"segment_seek\",le=\"25\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_sort_candidate_count_sum{query_type=\"search\",plan=\"sorted_segment_seek\",exactness=\"exact\",source=\"sorted_segment_scan\",cursor_mode=\"segment_seek\"} 13") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_sort_selected_count_bucket{query_type=\"search\",plan=\"sorted_segment_seek\",exactness=\"exact\",source=\"sorted_segment_scan\",cursor_mode=\"segment_seek\",le=\"5\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_sort_selected_count_sum{query_type=\"search\",plan=\"sorted_segment_seek\",exactness=\"exact\",source=\"sorted_segment_scan\",cursor_mode=\"segment_seek\"} 5") != null);
}

test "sort metrics expose bounded labels without table or field dimensions" {
    var collector = Collector.init(std.testing.allocator);
    defer collector.deinit();

    try collector.observeSortProfile(.search, .{
        .plan = "native_doc_values_top_n",
        .exactness = "exact",
        .source = "doc_values_collector",
        .cursor_support = "comparator",
        .candidate_count = 100,
        .selected_count = 10,
        .total_us = 500,
        .sort_rejection_field = types.SortProfileField.init("tenant_supplied_field"),
    });
    try collector.observeSortFailure(.search, "missing_doc_values_coverage");

    var writer_buf: [32768]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try collector.writePrometheus(&writer);
    const output = writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_sort_plan_total{query_type=\"search\",plan=\"native_doc_values_top_n\",exactness=\"exact\",source=\"doc_values_collector\",cursor_mode=\"comparator\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_indexes_sort_failures_total{query_type=\"search\",reason=\"missing_doc_values_coverage\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Name=") == null);
    try std.testing.expect(std.mem.indexOf(u8, output, "field=") == null);
    try std.testing.expect(std.mem.indexOf(u8, output, "tenant_supplied_field") == null);
}
