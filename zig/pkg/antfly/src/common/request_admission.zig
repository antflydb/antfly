// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const prometheus = @import("prometheus.zig");

pub const Class = enum { none, query, write, inference };

pub const PrometheusClass = enum {
    query,
    write,
    inference,
};

/// Shared process-local owner. Legacy calls remain fail-fast and zero capacity
/// remains unlimited. Contextual calls may opt into bounded waiting.
pub const workload = @import("workload_admission.zig");
pub const RequestAdmission = workload.Controller;

/// Emit the stable process-level metrics for a foreground admission class.
/// Keeping names here prevents runtime-specific health endpoints from drifting.
pub fn appendPrometheusMetrics(
    writer: *std.Io.Writer,
    comptime class: PrometheusClass,
    stats: RequestAdmission.Stats,
) !void {
    const names = switch (class) {
        .query => .{
            .capacity = "antfly_admission_query_capacity_requests",
            .in_flight = "antfly_admission_query_in_flight_requests",
            .peak_in_flight = "antfly_admission_query_peak_in_flight_requests",
            .rejected_total = "antfly_admission_query_rejected_requests_total",
            .capacity_help = "Maximum concurrent expensive public queries; zero means unlimited",
            .in_flight_help = "Currently executing expensive public queries",
            .peak_help = "Peak concurrent expensive public queries since process start",
            .rejected_help = "Public queries rejected by admission control",
        },
        .write => .{
            .capacity = "antfly_admission_write_capacity_requests",
            .in_flight = "antfly_admission_write_in_flight_requests",
            .peak_in_flight = "antfly_admission_write_peak_in_flight_requests",
            .rejected_total = "antfly_admission_write_rejected_requests_total",
            .capacity_help = "Maximum concurrent foreground data mutations; zero means unlimited",
            .in_flight_help = "Currently executing foreground data mutations",
            .peak_help = "Peak concurrent foreground data mutations since process start",
            .rejected_help = "Foreground data mutations rejected by admission control",
        },
        .inference => .{
            .capacity = "antfly_admission_inference_capacity_requests",
            .in_flight = "antfly_admission_inference_in_flight_requests",
            .peak_in_flight = "antfly_admission_inference_peak_in_flight_requests",
            .rejected_total = "antfly_admission_inference_rejected_requests_total",
            .capacity_help = "Maximum concurrent inference requests; zero means unlimited",
            .in_flight_help = "Inference requests currently admitted",
            .peak_help = "Peak concurrent inference requests since process start",
            .rejected_help = "Inference requests rejected by admission control",
        },
    };
    try prometheus.appendPromMetric(writer, names.capacity, "gauge", names.capacity_help, stats.capacity);
    try prometheus.appendPromMetric(writer, names.in_flight, "gauge", names.in_flight_help, stats.in_flight);
    try prometheus.appendPromMetric(writer, names.peak_in_flight, "gauge", names.peak_help, stats.peak_in_flight);
    try prometheus.appendPromMetric(writer, names.rejected_total, "counter", names.rejected_help, stats.rejected_total);
    const prefix = "antfly_admission_" ++ @tagName(class);
    try prometheus.appendPromMetric(writer, prefix ++ "_queue_capacity_requests", "gauge", "Configured maximum queued requests", stats.max_queued_requests);
    try prometheus.appendPromMetric(writer, prefix ++ "_queue_capacity_bytes", "gauge", "Configured maximum queued request bytes", stats.max_queued_bytes);
    try prometheus.appendPromMetric(writer, prefix ++ "_retained_capacity_bytes", "gauge", "Configured request reservation byte ceiling; zero is unlimited", stats.max_retained_bytes);
    try prometheus.appendPromMetric(writer, prefix ++ "_wait_ceiling_milliseconds", "gauge", "Configured admission wait ceiling; zero is fail fast", stats.max_wait_ms);
    try prometheus.appendPromMetric(writer, prefix ++ "_draining", "gauge", "Whether this admission owner is closed to new work", @intFromBool(stats.draining));
    try prometheus.appendPromMetric(writer, prefix ++ "_queued_requests", "gauge", "Requests waiting for admission", stats.queued);
    try prometheus.appendPromMetric(writer, prefix ++ "_queued_bytes", "gauge", "Retained bytes owned by admission waiters", stats.queued_bytes);
    try prometheus.appendPromMetric(writer, prefix ++ "_retained_bytes", "gauge", "Request bytes reserved by queued and active leases", stats.retained_bytes);
    try prometheus.appendPromMetric(writer, prefix ++ "_waited_requests_total", "counter", "Requests entering admission waiting", stats.waited_total);
    try prometheus.appendPromMetric(writer, prefix ++ "_wait_nanoseconds_total", "counter", "Cumulative admission waiting time", stats.wait_ns_total);
    try prometheus.appendPromMetric(writer, prefix ++ "_expired_requests_total", "counter", "Waiters retired on admission or request deadline", stats.expired_total);
    try prometheus.appendPromMetric(writer, prefix ++ "_cancelled_requests_total", "counter", "Waiters retired on cancellation", stats.cancelled_total);
    const histogram = prefix ++ "_wait_seconds";
    try writer.print("# HELP {s} Admission queue residence time\n# TYPE {s} histogram\n", .{ histogram, histogram });
    inline for (workload.wait_bucket_seconds, 0..) |bound, i| {
        try writer.print("{s}_bucket{{le=\"{s}\"}} {d}\n", .{ histogram, bound, stats.wait_buckets[i] });
    }
    try writer.print("{s}_sum {d}\n{s}_count {d}\n", .{ histogram, @as(f64, @floatFromInt(stats.wait_ns_total)) / std.time.ns_per_s, histogram, stats.wait_completed_total });
}

test "request admission bounds positive capacity and preserves unlimited mode" {
    var bounded = RequestAdmission.init(1);
    try std.testing.expect(bounded.tryAcquire());
    try std.testing.expect(!bounded.tryAcquire());
    bounded.release();
    try std.testing.expectEqual(@as(u64, 1), bounded.stats().rejected_total);

    var unlimited = RequestAdmission.init(0);
    try std.testing.expect(unlimited.tryAcquire());
    try std.testing.expect(unlimited.tryAcquire());
    unlimited.release();
    unlimited.release();
    try std.testing.expectEqual(@as(usize, 2), unlimited.stats().peak_in_flight);
}

test "request admission lease releases exactly once" {
    var admission = RequestAdmission.init(1);
    var lease = admission.tryAcquireLease() orelse return error.TestUnexpectedResult;
    try std.testing.expect(admission.tryAcquireLease() == null);
    try std.testing.expectEqual(@as(usize, 1), admission.stats().in_flight);
    lease.release();
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), admission.stats().in_flight);
    var second = admission.tryAcquireLease() orelse return error.TestUnexpectedResult;
    second.release();
}

test "request admission metrics use the shared admission namespace" {
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, .query, .{
        .capacity = 32,
        .in_flight = 2,
        .peak_in_flight = 4,
        .rejected_total = 1,
    });
    try appendPrometheusMetrics(&output.writer, .write, .{
        .capacity = 16,
        .in_flight = 3,
        .peak_in_flight = 5,
        .rejected_total = 2,
    });
    try appendPrometheusMetrics(&output.writer, .inference, .{
        .capacity = 8,
        .in_flight = 1,
        .peak_in_flight = 3,
        .rejected_total = 4,
    });
    const rendered = output.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_query_capacity_requests 32\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_query_rejected_requests_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_write_capacity_requests 16\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_write_rejected_requests_total 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_inference_capacity_requests 8\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_inference_rejected_requests_total 4\n") != null);
}
