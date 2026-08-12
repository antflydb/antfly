// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const prometheus = @import("prometheus.zig");

pub const PrometheusClass = enum {
    query,
    write,
};

/// Process-local, fail-fast admission for one foreground request class.
/// A zero capacity disables the limit while retaining accounting.
pub const RequestAdmission = struct {
    capacity: usize,
    in_flight: std.atomic.Value(usize) = .init(0),
    rejected_total: std.atomic.Value(u64) = .init(0),
    peak_in_flight: std.atomic.Value(usize) = .init(0),

    pub fn init(capacity: usize) RequestAdmission {
        return .{ .capacity = capacity };
    }

    pub fn tryAcquire(self: *RequestAdmission) bool {
        var observed = self.in_flight.load(.acquire);
        while (self.capacity == 0 or observed < self.capacity) {
            if (self.in_flight.cmpxchgWeak(observed, observed + 1, .acq_rel, .acquire) == null) {
                const admitted = observed + 1;
                var peak = self.peak_in_flight.load(.acquire);
                while (peak < admitted) {
                    if (self.peak_in_flight.cmpxchgWeak(peak, admitted, .acq_rel, .acquire) == null) break;
                    peak = self.peak_in_flight.load(.acquire);
                }
                return true;
            }
            observed = self.in_flight.load(.acquire);
        }
        _ = self.rejected_total.fetchAdd(1, .monotonic);
        return false;
    }

    pub fn release(self: *RequestAdmission) void {
        _ = self.in_flight.fetchSub(1, .acq_rel);
    }

    pub const Stats = struct {
        capacity: usize,
        in_flight: usize,
        peak_in_flight: usize,
        rejected_total: u64,
    };

    pub fn stats(self: *const RequestAdmission) Stats {
        return .{
            .capacity = self.capacity,
            .in_flight = self.in_flight.load(.acquire),
            .peak_in_flight = self.peak_in_flight.load(.acquire),
            .rejected_total = self.rejected_total.load(.acquire),
        };
    }
};

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
            .capacity_help = "Maximum concurrent expensive public queries",
            .in_flight_help = "Currently executing expensive public queries",
            .peak_help = "Peak concurrent expensive public queries since process start",
            .rejected_help = "Public queries rejected by admission control",
        },
        .write => .{
            .capacity = "antfly_admission_write_capacity_requests",
            .in_flight = "antfly_admission_write_in_flight_requests",
            .peak_in_flight = "antfly_admission_write_peak_in_flight_requests",
            .rejected_total = "antfly_admission_write_rejected_requests_total",
            .capacity_help = "Maximum concurrent foreground data mutations",
            .in_flight_help = "Currently executing foreground data mutations",
            .peak_help = "Peak concurrent foreground data mutations since process start",
            .rejected_help = "Foreground data mutations rejected by admission control",
        },
    };
    try prometheus.appendPromMetric(writer, names.capacity, "gauge", names.capacity_help, stats.capacity);
    try prometheus.appendPromMetric(writer, names.in_flight, "gauge", names.in_flight_help, stats.in_flight);
    try prometheus.appendPromMetric(writer, names.peak_in_flight, "gauge", names.peak_help, stats.peak_in_flight);
    try prometheus.appendPromMetric(writer, names.rejected_total, "counter", names.rejected_help, stats.rejected_total);
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

test "request admission metrics use the shared admission namespace" {
    var output: [2048]u8 = undefined;
    var writer = std.Io.Writer.fixed(&output);
    try appendPrometheusMetrics(&writer, .query, .{
        .capacity = 32,
        .in_flight = 2,
        .peak_in_flight = 4,
        .rejected_total = 1,
    });
    try appendPrometheusMetrics(&writer, .write, .{
        .capacity = 16,
        .in_flight = 3,
        .peak_in_flight = 5,
        .rejected_total = 2,
    });
    const rendered = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_query_capacity_requests 32\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_query_rejected_requests_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_write_capacity_requests 16\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_admission_write_rejected_requests_total 2\n") != null);
}
