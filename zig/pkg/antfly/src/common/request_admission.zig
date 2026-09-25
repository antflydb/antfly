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
    try prometheus.appendPromMetric(writer, prefix ++ "_diagnostics_available", "gauge", "Whether the statistics provider exposes policy and limiting-resource diagnostics", @intFromBool(stats.policy_generation != 0));
    if (stats.policy_generation != 0) {
        try prometheus.appendPromMetric(writer, prefix ++ "_policy_generation", "gauge", "Process-local policy revision; resets on restart and advances on accepted reconfiguration", stats.policy_generation);
        try prometheus.appendPromMetric(writer, prefix ++ "_outstanding_requests", "gauge", "Active plus queued foreground operations; excludes output retained after execution", stats.in_flight +| stats.queued);
        const rejections = prefix ++ "_rejections_by_reason_total";
        try writer.print("# HELP {s} Admission rejections by bounded limiting reason\n# TYPE {s} counter\n", .{ rejections, rejections });
        inline for (@typeInfo(workload.RejectionReason).@"enum".fields, 0..) |field, i| {
            try writer.print("{s}{{reason=\"{s}\"}} {d}\n", .{ rejections, field.name, stats.rejection_reasons[i] });
        }
        const denials = prefix ++ "_allocation_denials_total";
        try writer.print("# HELP {s} Tracked allocation attempts denied by a budget; distinct from rejected requests\n# TYPE {s} counter\n", .{ denials, denials });
        inline for (@typeInfo(workload.AllocationDenial).@"enum".fields, 0..) |field, i| {
            try writer.print("{s}{{reason=\"{s}\"}} {d}\n", .{ denials, field.name, stats.allocation_denials[i] });
        }
    }
    try prometheus.appendPromMetric(writer, prefix ++ "_queue_capacity_requests", "gauge", "Configured maximum queued requests", stats.max_queued_requests);
    try prometheus.appendPromMetric(writer, prefix ++ "_queue_capacity_bytes", "gauge", "Configured maximum queued request bytes", stats.max_queued_bytes);
    try prometheus.appendPromMetric(writer, prefix ++ "_retained_capacity_bytes", "gauge", "Configured request and tracked allocation byte ceiling; zero is unlimited", stats.max_retained_bytes);
    try prometheus.appendPromMetric(writer, prefix ++ "_wait_ceiling_milliseconds", "gauge", "Configured admission wait ceiling; zero is fail fast", stats.max_wait_ms);
    try prometheus.appendPromMetric(writer, prefix ++ "_draining", "gauge", "Whether this admission owner is closed to new work", @intFromBool(stats.draining));
    try prometheus.appendPromMetric(writer, prefix ++ "_queued_requests", "gauge", "Requests waiting for admission", stats.queued);
    try prometheus.appendPromMetric(writer, prefix ++ "_queued_bytes", "gauge", "Retained bytes owned by admission waiters", stats.queued_bytes);
    try prometheus.appendPromMetric(writer, prefix ++ "_retained_bytes", "gauge", "Request reservations and tracked query or output allocations still owned", stats.retained_bytes);
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
    // Partial inference/legacy providers must not advertise zero pressure as
    // though they supplied complete diagnostics.
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_inference_diagnostics_available 0\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_inference_rejections_by_reason_total") == null);
}

test "request admission metrics expose actual fixed policy and bounded failure reasons" {
    var gate = RequestAdmission.initConfigured(1, .{ .max_retained_bytes = 128 });
    var lease = try gate.acquire(.{ .io = std.testing.io, .retained_bytes = 64 });
    defer lease.release();
    try std.testing.expect(!gate.tryAcquire());
    try std.testing.expectError(error.AdmissionBytesExhausted, gate.reserveMemory(65));
    try gate.reconfigure(2, .{ .max_retained_bytes = 128 });
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try appendPrometheusMetrics(&output.writer, .query, gate.stats());
    const rendered = output.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_query_policy_generation 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_query_capacity_requests 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_query_outstanding_requests 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_query_rejections_by_reason_total{reason=\"execution_capacity\"} 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_query_allocation_denials_total{reason=\"retained_bytes\"} 1\n") != null);
}
